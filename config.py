from .Core.Handler import Handler

agent = Handler()


def test():
    return 1


__all__ = [
    'agent',
    'test',
]
