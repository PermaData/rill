from rill.utils import Annotation, FlagAnnotation
import pytest


class AddAThing(Annotation):
    multi = True
    default = []
    attribute = 'list_of_things'


class BooleanThing(FlagAnnotation):
    pass


class NamedThing(Annotation):
    pass


def test_annotation_basic():
    @AddAThing(2)
    @AddAThing(3)
    @BooleanThing
    @NamedThing("this")
    class This(object):
        pass

    assert AddAThing.get(This) == [2, 3]

    assert BooleanThing.get(This) is True
    assert NamedThing.get(This) == 'this'


def test_annotation_defaults():
    class NotDecorated(object):
        pass

    assert BooleanThing.get(NotDecorated) is False
    assert NamedThing.get(NotDecorated) is None
    assert AddAThing.get(NotDecorated) == []


def test_annotation_inherited_defaults():
    @AddAThing(1)
    @AddAThing(2)
    class ClassA(object):
        list_of_things = []

    @AddAThing(3)
    @AddAThing(4)
    class ClassB(object):
        pass

    class ClassC(ClassB):
        pass

    @AddAThing(5)
    @AddAThing(6)
    class ClassD(ClassC):
        pass

    @AddAThing(7)
    class ClassE(ClassD, ClassA):
        pass

    assert AddAThing.get(ClassA) == [1, 2]
    assert AddAThing.get_inherited(ClassA) == [1, 2]

    assert AddAThing.get(ClassB) == [3, 4]
    assert AddAThing.get_inherited(ClassB) == [3, 4]

    assert AddAThing.get(ClassC) == []
    assert AddAThing.get_inherited(ClassC) == [3, 4]

    assert AddAThing.get(ClassD) == [5, 6]
    assert AddAThing.get_inherited(ClassD) == [3, 4, 5, 6]
    assert AddAThing.get_inherited(ClassE) == [1, 2, 3, 4, 5, 6, 7]


def test_multi_annotation_error():
    with pytest.raises(ValueError):

        @NamedThing("this")
        @NamedThing("that")
        class NotDecorated(object):
            pass
