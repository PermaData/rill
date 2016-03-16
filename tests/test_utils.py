from rill.engine.utils import Annotation, FlagAnnotation
import pytest


class ListOfThings(Annotation):
    multi = True
    default = []

    def __init__(self, this, that=None):
        data = dict(this=this, that=that)
        super(ListOfThings, self).__init__(data)


class BooleanThing(FlagAnnotation):
    pass


class NamedThing(Annotation):
    pass


def test_annotation_basic():
    @ListOfThings(2)
    @ListOfThings(3, 4)
    @BooleanThing
    @NamedThing("this")
    class This(object):
        pass

    assert ListOfThings.get(This) == [
        {'this': 2, 'that': None}, {'this': 3, 'that': 4}]

    assert BooleanThing.get(This) is True
    assert NamedThing.get(This) == 'this'


def test_annotation_defaults():
    class NotDecorated(object):
        pass

    assert BooleanThing.get(NotDecorated) is False
    assert NamedThing.get(NotDecorated) is None
    assert ListOfThings.get(NotDecorated) == []


def test_multi_annotation_error():
    with pytest.raises(ValueError):

        @NamedThing("this")
        @NamedThing("that")
        class NotDecorated(object):
            pass
