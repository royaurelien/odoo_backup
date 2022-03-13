
import os
from celery.result import AsyncResult



def unpack_chain(nodes):
    while nodes:
        yield nodes
        nodes = nodes.children
    yield nodes


def unpack_parents(nodes):
    while nodes.parent:
        yield nodes.parent
        nodes = nodes.parent
    yield nodes


def iter_children(node):
    if node.children:
        for child in node.children:
            # yield child
            yield from iter_children(child)

    yield node


def store(node):
    """
    Get all tasks id's from chain
    """
    id_chain = []
    while node.parent:
      id_chain.append(node.id)
      node = node.parent
    id_chain.append(node.id)
    return id_chain


def _get_file_from_task(task_id):
    task_result = AsyncResult(task_id)
    if not task_result.result:
        raise ValueError('No task found motherf****r !')

    filepath = task_result.result.get('download', False)

    if not os.path.isfile(filepath):
        raise ValueError('No file found at {}'.format(filepath))

    return filepath


def iterfile(path):
    with open(path, mode="rb") as file_like:
        yield from file_like

