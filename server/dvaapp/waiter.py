from models import TEvent
import logging


class Waiter(object):

    def __init__(self, task):
        self.task = task
        self.reduce_target = self.task.arguments['reduce_target']
        self.reduce_filter = self.task.arguments.get('reduce_filter', [])

    def is_complete(self):
        if self.reduce_target == 'root':
            return self.quick_is_complete_root()
        elif self.reduce_target == 'all':
            return self.quick_is_complete_root()
        elif self.reduce_target == 'filter':
            raise NotImplementedError
        else:
            raise ValueError("{} invalid reduce_target".format(self.reduce_target))

    def quick_is_complete_root(self):
        for t in TEvent.objects.filter(parent_id=self.task.pk):
            # Don't wait on perform_reduce for the root to prevent deadlock (i.e. one task waiting on another)
            if not (t.completed or t.errored) and t.operation != 'perform_reduce':
                logging.info(
                    "Returning false {} running {} on {} has not yet completed/failed".format(t.pk, t.operation,
                                                                                              t.queue))
                return False
        return True

    def quick_is_complete_all(self):
        if self.quick_is_complete_root():
            for t in TEvent.objects.filter(parent_id=self.task.pk):
                if t.operation != 'perform_reduce':  # Don't wait on perform_reduce child_tasks to prevent deadlock
                    if not self.check_if_task_children_are_complete_recursive(t.pk):
                        return False
        else:
            return False
        return True

    def check_if_task_children_are_complete_recursive(self, task_id):
        """
        This is intentionally different since we DO wish to wait on reduce tasks performed by child tasks.
        :param task_id:
        :return:
        """
        for t in TEvent.objects.filter(parent_id=task_id):
            if not (t.completed or t.errored):
                logging.info(
                    "Returning false {} running {} on {} has not yet completed/failed".format(t.pk, t.operation,
                                                                                              t.queue))
                return False
            if not self.check_if_task_children_are_complete_recursive(t.pk):
                return False
        return True
