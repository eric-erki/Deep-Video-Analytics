from models import TEvent
import logging


class Waiter(object):

    def __init__(self, task):
        self.task = task
        self.reduce_target = self.task.arguments['reduce_target']
        self.reduce_filter = self.task.arguments.get('reduce_filter', [])
        self.root_group_id = self.task.parent.task_group_id
        self.filter_set = set()
        self.task_group_name_to_index = self.task.parent_process.script.get('task_group_name_to_index',{})
        self.parent_task_group_index = self.task.parent_process.script.get('parent_task_group_index', {})
        for task_name in self.reduce_filter:
            task_group_id = self.task_group_name_to_index[task_name]
            self.filter_set.add(int(task_group_id))
            self.add_parent_groups(task_group_id)
        if self.reduce_filter:
            logging.info("The waiter will wait on following task groups : {}".format(self.filter_set))

    def add_parent_groups(self,task_group_id):
        if str(task_group_id) in self.parent_task_group_index:
            parent_group_id = self.parent_task_group_index[str(task_group_id)]
            self.filter_set.add(int(parent_group_id))
            if parent_group_id != self.root_group_id:
                self.add_parent_groups(parent_group_id)

    def is_complete(self):
        if self.reduce_target == 'root':
            logging.info("waiting only on immediate children of root task")
            return self.is_complete_root()
        elif self.reduce_target == 'all':
            logging.info("waiting on all children tasks and their children tasks.")
            return self.is_complete_all()
        elif self.reduce_target == 'filter':
            logging.info("waiting on subset of children tasks")
            return self.is_complete_filtered()
        else:
            raise ValueError("{} invalid reduce_target".format(self.reduce_target))

    def is_complete_root(self):
        for t in TEvent.objects.filter(parent_id=self.task.parent_id):
            # Don't wait on perform_reduce for the root to prevent deadlock (i.e. one task waiting on another)
            if not (t.completed or t.errored) and t.operation != 'perform_reduce':
                logging.info(
                    "Returning false {} running {} on {} has not yet completed/failed".format(t.pk, t.operation,
                                                                                              t.queue))
                return False
        return True

    def is_complete_root_filtered(self):
        for t in TEvent.objects.filter(parent_id=self.task.parent_id):
            # Don't wait on perform_reduce for the root to prevent deadlock (i.e. one task waiting on another)
            if t.task_group_id in self.filter_set:
                if not (t.completed or t.errored) and t.operation != 'perform_reduce':
                    logging.info(
                        "Returning false {} running {} on {} has not yet completed/failed".format(t.pk, t.operation,
                                                                                                  t.queue))
                    return False
        return True

    def is_complete_all(self):
        if self.is_complete_root():
            for t in TEvent.objects.filter(parent_id=self.task.parent_id):
                if t.operation != 'perform_reduce':  # Don't wait on perform_reduce child_tasks to prevent deadlock
                    if not self.check_if_task_children_are_complete_recursive(t.pk):
                        return False
        else:
            return False
        return True

    def is_complete_filtered(self):
        if self.is_complete_root_filtered():
            for t in TEvent.objects.filter(parent_id=self.task.parent_id):
                if t.operation != 'perform_reduce':  # Don't wait on perform_reduce child_tasks to prevent deadlock
                    if not self.check_if_task_children_are_complete_recursive_filtered(t.pk):
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

    def check_if_task_children_are_complete_recursive_filtered(self, task_id):
        """
        :param task_id:
        :return:
        """
        for t in TEvent.objects.filter(parent_id=task_id):
            logging.info("{} in filter_set: {}".format(t.task_group_id,self.filter_set))
            if t.task_group_id in self.filter_set:
                if not (t.completed or t.errored):
                    logging.info(
                        "Returning false {} running {} on {} from task group {} has not "
                        "yet completed/failed".format(t.pk, t.operation, t.queue, t.task_group_id))
                    return False
                if not self.check_if_task_children_are_complete_recursive_filtered(t.pk):
                    return False
        return True
