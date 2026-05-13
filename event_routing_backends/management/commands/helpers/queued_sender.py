"""
Class to handle batching and sending bulk transformed statements.
"""
import datetime
import json
import logging
import os
import sys
from io import BytesIO
from time import sleep

from eventtracking.tracker import get_tracker

from event_routing_backends.management.commands.helpers.event_log_parser import parse_json_event


class QueuedSender:
    """
    Handles queuing and sending events to the destination.
    """
    def __init__(  # pylint: disable=too-many-positional-arguments
        self,
        destination,
        destination_container,
        destination_prefix,
        transformer_type,
        max_queue_size=10000,
        sleep_between_batches_secs=1.0,
        dry_run=False,
        lrs_urls=None
    ):
        self.destination = destination
        self.destination_container = destination_container
        self.destination_prefix = destination_prefix
        self.transformer_type = transformer_type
        self.event_queue = []
        self.max_queue_size = max_queue_size
        self.sleep_between_batches = sleep_between_batches_secs
        self.dry_run = dry_run
        self.lrs_urls = lrs_urls or []

        # Bookkeeping
        self.queued_lines = 0
        self.skipped_lines = 0
        self.unparsable_lines = 0
        self.batches_sent = 0

        self.tracker = get_tracker()
        self.engine = self.tracker.backends["event_transformer"]
        self.backend = self.engine.backends[self.transformer_type]

    def is_known_event(self, event):
        """
        Check whether any processor cares about this event.
        """
        if "name" in event:
            for processor in self.engine.processors:
                if hasattr(processor, 'whitelist') and event["name"] in processor.whitelist:
                    return True
                elif hasattr(processor, 'registry') and event["name"] in processor.registry.mapping:
                    return True
        return False

    def transform_and_queue(self, line):
        """
        Queue the JSON representation of this log line, if valid and known to any processor.
        """
        event = parse_json_event(line)

        if not event:
            self.unparsable_lines += 1
            return

        if not self.is_known_event(event):
            self.skipped_lines += 1
            return

        self.queue(event)
        self.queued_lines += 1

    def _process_for_logger(self):
        """
        Transform queued events and write each xAPI/Caliper statement as a JSON line to stdout.

        Bypasses the processor chain (which is gated by XAPI_EVENTS_ENABLED / RouterConfiguration)
        and calls the registry directly — the only goal is serialised statements on stdout for
        Vector (or any line-oriented consumer) to pick up.  Status messages go to stderr.
        """
        from eventtracking.processors.exceptions import NoTransformerImplemented

        registry = next(
            (p.registry for p in self.backend.processors if getattr(p, 'registry', None)),
            None,
        )
        if not registry:
            print(f"No transformer registry found for backend {self.transformer_type}", file=sys.stderr)
            return

        print(f"Transforming {len(self.event_queue)} events for logger...", file=sys.stderr)
        for event in self.event_queue:
            try:
                transformer = registry.get_transformer(event)
                transformed = transformer.transform()
            except NoTransformerImplemented:
                continue
            except Exception as exc:
                print(f"Error transforming {event.get('name')}: {exc}", file=sys.stderr)
                continue

            if not isinstance(transformed, list):
                transformed = [transformed]
            xapi_logger = logging.getLogger('xapi_tracking')
            for stmt in transformed:
                if stmt and getattr(getattr(stmt, 'object', None), 'id', None):
                    xapi_logger.info(stmt.to_json())

    def queue(self, event):
        """
        Add an event to the queue, try to send if we've reached our batch size.
        """
        self.event_queue.append(event)
        if len(self.event_queue) == self.max_queue_size:
            if self.dry_run:
                print("Dry run, skipping, but still clearing the queue.")
            else:
                print(f"Max queue size of {self.max_queue_size} reached, sending.", file=sys.stderr)
                if self.destination == "LRS":
                    self.send()
                elif self.destination == "LOGGER":
                    self._process_for_logger()
                else:
                    self.store()

                self.batches_sent += 1
            self.event_queue.clear()
            sleep(self.sleep_between_batches)

    def send(self):
        """
        Send to the LRS if we're configured for that.

        Events are converted to the output xAPI / Caliper format in the router.
        A no-op for LOGGER destination (logging fires through processors instead).
        """
        if self.destination == "LRS":
            print(f"Sending {len(self.event_queue)} events to LRS...")
            self.backend.bulk_send(self.event_queue, self.lrs_urls)
        elif self.destination == "LOGGER":
            pass
        else:
            print("Skipping send, we're storing with libcloud instead of an LRS.")

    def store(self):
        """
        Store to a libcloud destination if we're configured for that.

        Events are converted to the output xAPI / Caliper format here before being saved.
        """
        if self.destination == "LRS":
            print("Store is being called on an LRS destination, skipping.")
            return

        display_path = os.path.join(self.destination_container, self.destination_prefix.lstrip("/"))
        print(f"Storing {len(self.event_queue)} events to libcloud destination {display_path}")

        container = self.destination.get_container(self.destination_container)

        datestr = datetime.datetime.now().strftime('%y-%m-%d_%H-%M-%S')
        object_name = f"{self.destination_prefix}/{datestr}_{self.transformer_type}.log"
        print(f"Writing to {self.destination_container}/{object_name}")

        out = BytesIO()
        for event in self.event_queue:
            transformed_event = self.engine.processors[0](event)
            out.write(str.encode(json.dumps(transformed_event)))
            out.write(str.encode("\n"))
        out.seek(0)

        self.destination.upload_object_via_stream(
            out,
            container,
            object_name
        )

    def finalize(self):
        """
        Send a last batch of events via the LRS, or store a complete set of events to a libcloud destination.
        """
        print(f"Finalizing {len(self.event_queue)} events to {self.destination}")
        if not self.queued_lines:
            print("Nothing in the queue to store!")
        elif self.dry_run:
            print("Dry run, skipping final storage.")
        else:
            # One final send, in case there are events left in the queue
            if self.destination is None or self.destination == "LRS":
                print("Sending to LRS!")
                self.send()
            elif self.destination == "LOGGER":
                print("Processing for logger!", file=sys.stderr)
                self._process_for_logger()
            else:
                print("Storing via Libcloud!")
                self.store()
            self.batches_sent += 1

        print(f"Queued {self.queued_lines} log lines, "
              f"could not parse {self.unparsable_lines} log lines, "
              f"skipped {self.skipped_lines} log lines, "
              f"sent {self.batches_sent} batches.")
