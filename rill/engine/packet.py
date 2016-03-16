class Chain(object):
    def __init__(self, name):
        self.name = name
        self.members = []


class Packet(object):
    """An Information Packet.

    May either contain arbitrary data, when `type` is `NORMAL`, or a sentinel
    string, when `type` is not `NORMAL`.  The latter case is used for things
    like open and close brackets
    """

    # FIXME: replace with enum34?
    OPEN = object()
    CLOSE = object()
    NORMAL = object()

    def __init__(self, content, owner, type=NORMAL):
        self._content = content
        self.owner = None
        self._type = type
        # dict of {str: object}
        self.attrs = {}
        # dict of {str: Chain}
        self.chains = {}
        self.set_owner(owner)

    def clear_owner(self):
        """Clear the owner of a Packet.

        If the owner is a Component, this reduces the number of Packets that it
        owns
        """
        from rill.engine.component import Component
        if isinstance(self.owner, Component):
            self.owner._packet_count -= 1
        self.owner = None

    # def get_attribute(self, key):
    #     """Get named attribute of Packet
    #     """
    #     return self._attrs.get(key)
    #
    # def get_attributes(self):
    #     """Get all attributes of this Packet
    #     """
    #     return self._attrs.keys()

    def get_chain(self, name):
        """Get named chain
        """
        chain = self.chains.get(name)
        if chain is not None:
            return chain.members
        return []

    def get_chains(self):
        """Get all chains for this Packet
        """
        return self.chains.keys()

    def get_content(self):
        """Get packet's contents
        """
        # if (self._type == NORMAL)
        return self._content
        # else
        # return None

    def consume(self):
        """Get packet's contents and drop it.

        Convenience method for a common pattern.
        """
        content = self.get_content()
        self.drop()
        return content

    def get_name(self):
        if self._type == self.NORMAL:
            return None

        return self._content

    def get_root(self):
        """Get root owner of this Packet.

        This follows the owner chain up until it finds the root component.

        Returns
        -------
        ``rill.engine.component.Component``
        """
        p = self
        while isinstance(p, Packet):
            p = p.owner
        return p

    def get_type(self):
        """Get the type of the Packet
        """
        return self._type

    # def put_attribute(self, key, value):
    #     """Make an Object a named attribute of a Packet
    #     """
    #     self._attrs[key] = value
    #
    # def remove_attribute(self, key):
    #     """Remove a named attribute from a Packet (does not return the attribute)
    #     """
    #     self._attrs.pop(key)

    def set_owner(self, new_owner):
        """Change the owner of a Packet.

        If `new_owner` is a Component, increment the number of Packets owned by
        that Component (when the Component is deactivated, it must no longer
        own any Packets)

        Parameters
        ----------
        new_owner : ``rill.engine.component.Component`` or
            ``rill.engine.packet.Packet``
        """
        from rill.engine.component import Component
        self.clear_owner()
        self.owner = new_owner
        if isinstance(self.owner, Component):
            self.owner._packet_count += 1  # count of owned packets

    #
    def drop(self):
        """
        Drop this packet.

        This exists primarily for writing functional components and should not
        be called outside the context of `Component.execute`
        """
        # The component that this runs from doesn't matter: the component is
        # not referenced nor is it used to determine the logger display
        return self.get_root().drop(self)

    def clone(self):
        # FIXME: clone attrs and chains
        return Packet(self._content, self.owner, self._type)

    def __str__(self):
        value = "None"
        names = {
            self.NORMAL: "NORMAL",
            self.OPEN: "OPEN",
            self.CLOSE: "CLOSE"
        }
        if self.get_type() == self.NORMAL:
            obj = self.get_content()
            if obj is not None:
                value = repr(obj)
        else:
            value = names[self.get_type()]
            value += " " + self.get_name()
        return value
