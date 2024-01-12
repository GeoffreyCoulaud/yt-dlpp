from abc import abstractmethod
from multiprocessing import JoinableQueue, Process
import sys
from typing import Generic, Optional, Sequence, TypeVar

InputItemT = TypeVar("InputItemT")
OutputItemT = TypeVar("OutputItemT")


class BaseWorker(Process, Generic[InputItemT, OutputItemT]):
    """Worker process with input and output queues"""

    input_queue: JoinableQueue
    output_queue: Optional[JoinableQueue]

    def __init__(
        self, input_queue: JoinableQueue, output_queue: Optional[JoinableQueue]
    ) -> None:
        super().__init__(daemon=True)
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self) -> None:
        while True:
            # Get input item
            item: Optional[InputItemT] = self.input_queue.get()

            # Process it
            if item is not None:
                results = self._process_item(item)
                if self.output_queue is not None:
                    for result in results:
                        self.output_queue.put(result)

            # Signal that we finished processing
            self.input_queue.task_done()

            # Stop if requested to
            if item is None:
                break

        # Exit gracefuly
        sys.exit(0)

    @abstractmethod
    def _process_item(self, item: InputItemT) -> Sequence[OutputItemT]:
        """Process an item from the queue and return results to the output queue."""
