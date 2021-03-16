# -*- coding: utf-8 -*-
# *********************************************************************
# lewis - a library for creating hardware device simulators
# Copyright (C) 2016-2020 European Spallation Source ERIC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# *********************************************************************

import asynchat
import asyncore
import inspect
import re
import socket

from scanf import scanf_compile

from lewis.core.adapters import Adapter
from lewis.core.devices import InterfaceBase
from lewis.core.logging import has_log
from lewis.core.utils import format_doc_text


@has_log
class StreamHandler(asynchat.async_chat):
    def __init__(self, sock, target, stream_server):
        asynchat.async_chat.__init__(self, sock=sock)
        self.set_terminator(target.in_terminator.encode())
        self._readtimeout = target.readtimeout
        self._readtimer = 0
        self._target = target
        self._buffer = []

        self._stream_server = stream_server
        self._target.handler = self

        self._set_logging_context(target)
        self.log.info("Client connected from %s:%s", *sock.getpeername())

    def process(self, msec):
        if not self._buffer:
            return

        if self._readtimer >= self._readtimeout and self._readtimeout != 0:
            if not self.get_terminator():
                # If no terminator is set, this timeout is the terminator
                self.found_terminator()
            else:
                self._readtimer = 0
                request = self._get_request()
                with self._stream_server.device_lock:
                    error = RuntimeError(
                        "ReadTimeout while waiting for command terminator."
                    )
                    reply = self._handle_error(request, error)
                self._send_reply(reply)

        if self._buffer:
            self._readtimer += msec

    def collect_incoming_data(self, data):
        self._buffer.append(data)
        self._readtimer = 0

    def _get_request(self):
        request = b"".join(self._buffer)
        self._buffer = []
        self.log.debug("Got request %s", request)
        return request

    def _push(self, reply):
        try:
            if isinstance(reply, str):
                if isinstance(self._target.out_terminator, bytes):
                    reply_message = reply.encode() + self._target.out_terminator
                else:
                    reply_message = (reply + self._target.out_terminator).encode()
            else:
                reply_message = reply + self._target.out_terminator
            self.push(reply_message)
        except TypeError as e:
            self.log.error("Problem creating reply, type error {}!".format(e))

    def _send_reply(self, reply):
        if reply is not None:
            self.log.debug("Sending reply %s", reply)
            self._push(reply)


    def _handle_error(self, request, error):
        self.log.debug("Error while processing request", exc_info=error)
        return self._target.handle_error(request, error)

    def found_terminator(self):
        self._readtimer = 0

        request = self._get_request()

        with self._stream_server.device_lock:
            try:
                cmd = next(
                    (
                        cmd
                        for cmd in self._target.bound_commands
                        if cmd.can_process(request)
                    ),
                    None,
                )

                if cmd is None:
                    raise RuntimeError("None of the device's commands matched.")

                self.log.info(
                    "Processing request %s using command %s",
                    request,
                    cmd.matcher.pattern,
                )

                reply = cmd.process_request(request)

            except Exception as error:
                reply = self._handle_error(request, error)

        self._send_reply(reply)

    def unsolicited_reply(self, reply):
        self.log.debug("Sending unsolicited reply %s", reply)
        self._push(reply)

    def handle_close(self):
        self.log.info("Closing connection to client %s:%s", *self.socket.getpeername())
        self._stream_server.remove_handler(self)
        asynchat.async_chat.handle_close(self)


@has_log
class StreamServer(asyncore.dispatcher):
    def __init__(self, host, port, target, device_lock):
        asyncore.dispatcher.__init__(self)
        self.target = target
        self.device_lock = device_lock
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)

        self._set_logging_context(target)
        self.log.info("Listening on %s:%s", host, port)

        self._accepted_connections = []

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            handler = StreamHandler(sock, self.target, self)

            self._accepted_connections.append(handler)

    def remove_handler(self, handler):
        self._accepted_connections.remove(handler)

    def close(self):
        # As this is an old style class, the base class method must
        # be called directly. This is important to still perform all
        # the teardown-work that asyncore.dispatcher does.
        self.log.info("Shutting down server, closing all remaining client connections.")
        asyncore.dispatcher.close(self)

        # But in addition, close all open sockets and clear the connection list.
        for handler in self._accepted_connections:
            handler.close()

        self._accepted_connections = []

    def process(self, msec):
        for handler in self._accepted_connections:
            handler.process(msec)


class PatternMatcher:
    """
    This class defines an interface for general command-matchers that use any kind of
    technique to match a certain request in string form. It is used by :class:`Func` to check
    whether a request can be processed using a function and to extract any function arguments.

    Sub-classes must implement all defined abstract methods/properties.

    .. seealso::

        :class:`regex`, :class:`scanf` are concrete implementations of this class.
    """

    def __init__(self, pattern):
        self._pattern = pattern

    @property
    def pattern(self):
        """The pattern definition used for matching a request."""
        return self._pattern

    @property
    def arg_count(self):
        """Number of arguments that are matched in a request."""
        raise NotImplementedError("The arg_count property must be implemented.")

    @property
    def argument_mappings(self):
        """Mapping functions that can be applied to the arguments returned by :meth:`match`."""
        raise NotImplementedError("The argument_mappings property must be implemented.")

    def match(self, request):
        """
        Tries to match the request against the internally stored pattern. Returns any matched
        function arguments.

        :param request: Request to attempt matching.
        :return: List of matched argument values (possibly empty) or None if not matching.
        """
        raise NotImplementedError("The match-method must be implemented.")


class regex(PatternMatcher):
    """
    Implementation of :class:`PatternMatcher` that compiles the specified pattern into a regular
    expression.
    """

    def __init__(self, pattern):
        super(regex, self).__init__(pattern)

        self.compiled_pattern = re.compile(pattern.encode())

    @property
    def arg_count(self):
        return self.compiled_pattern.groups

    @property
    def argument_mappings(self):
        return None

    def match(self, request):
        match = self.compiled_pattern.match(request)

        if match is None:
            return None

        return match.groups()


class scanf(regex):
    """
    Interprets the specified pattern as a scanf format. Internally, the scanf_ package is used
    to transform the format into a regular expression. Please consult the documentation of scanf_
    for valid pattern specifications.

    By default, the resulting regular expression matches exactly. Consider this example:

    .. sourcecode:: Python

        exact = scanf('T=%f')
        not_exact = scanf('T=%f', exact_match=False)

    The first pattern only matches the string ``T=4.0``, whereas the second would also match
    ``T=4.0garbage``. Please note that the specifiers like ``%f`` are automatically turned into
    groups in the generated regular expression.

    :param pattern: Scanf format specification.
    :param exact_match: Match only if the entire string matches.

    .. _scanf: https://github.com/joshburnett/scanf
    """

    def __init__(self, pattern, exact_match=True):
        self._scanf_pattern = pattern

        generated_regex, self._argument_mappings = scanf_compile(pattern)
        regex_pattern = generated_regex.pattern

        if exact_match:
            regex_pattern = "^{}$".format(regex_pattern)

        super(scanf, self).__init__(regex_pattern)

    @property
    def pattern(self):
        return self._scanf_pattern

    @property
    def argument_mappings(self):
        return self._argument_mappings


class Func:
    """
    Objects of this type connect a callable object to a pattern matcher (:class:`PatternMatcher`),
    which currently comprises :class:`regex` and :class:`scanf`. Strings are also
    accepted, they are treated like a regular expression internally. This preserves default
    behavior from older versions of Lewis.

    In general, Func-objects should not be created directly, instead they are created by one of
    the sub-classes of :class:`CommandBase` using :meth:`~CommandBase.bind`.

    Function arguments are indicated by groups in the regular expression. The number of
    groups has to match the number of arguments of the function. In earlier versions of Lewis it
    was possible to pass flags to ``re.compile``, this has been removed for consistency issues
    in :class:`Var`. It is however still possible to use the exact same flags as part of the
    regular expression. In the documentation of re_, this is outlined, simply add a group to the
    expression that contains the flags, for example ``(?i)`` to make the expression case
    insensitive. This special group does not count towards the matching groups used for argument
    capture.

    The optional argument_mappings can be an iterable of callables with one parameter of the
    same length as the number of arguments of the function. The first parameter will be
    transformed using the first function, the second using the second function and so on.
    This can be useful to automatically transform strings provided by the adapter into a proper
    data type such as ``int`` or ``float`` before they are passed to the function. In case the
    pattern is of type :class:`scanf`, this is optional (but will override the mappings
    provided by the matcher).

    The return_mapping argument is similar, it should map the return value of the function
    to a string. The default map function only does that when the supplied value
    is not None. It can also be set to a numeric value or a string constant so that the
    command always returns the same value. If it is ``None``, the return value is not
    modified at all.

    Finally, documentation can be provided by passing the doc-argument. If it is omitted,
    the docstring of the bound function is used and if that is not present, left empty.

    :param func: Function to be called when pattern matches or member of device/interface.
    :param pattern: :class:`regex`, :class:`scanf` object or string.
    :param argument_mappings: Iterable with mapping functions from string to some type.
    :param return_mapping: Mapping function for return value of method.
    :param doc: Description of the command. If not supplied, the docstring is used.

    .. _re: https://docs.python.org/2/library/re.html#regular-expression-syntax
    """

    def __init__(
        self, func, pattern, argument_mappings=None, return_mapping=None, doc=None
    ):
        if not callable(func):
            raise RuntimeError(
                "Can not construct a Func-object from a non callable object."
            )

        self.func = func

        if isinstance(pattern, str):
            pattern = regex(pattern)

        self.matcher = pattern

        if argument_mappings is None:
            argument_mappings = self.matcher.argument_mappings or None

        try:
            inspect.getcallargs(func, *[None] * self.matcher.arg_count)
        except TypeError:
            raise RuntimeError(
                "The number of arguments for function '{}' matched by pattern "
                "'{}' is not compatible with number of defined "
                "groups in pattern ({}).".format(
                    getattr(func, "__name__", repr(func)),
                    self.matcher.pattern,
                    self.matcher.arg_count,
                )
            )

        if argument_mappings is not None and (
            self.matcher.arg_count != len(argument_mappings)
        ):
            raise RuntimeError(
                "Supplied argument mappings for function matched by pattern '{}' specify {} "
                "argument(s), but the function has {} arguments.".format(
                    self.matcher, len(argument_mappings), self.matcher.arg_count
                )
            )

        self.argument_mappings = argument_mappings
        self.return_mapping = return_mapping
        self.doc = doc or (inspect.getdoc(self.func) if callable(self.func) else None)

    def can_process(self, request):
        return self.matcher.match(request) is not None

    def process_request(self, request):
        match = self.matcher.match(request)

        if match is None:
            raise RuntimeError("Request can not be processed.")

        args = self.map_arguments(match)

        return self.map_return_value(self.func(*args))

    def map_arguments(self, arguments):
        """
        Returns the mapped function arguments. If no mapping functions are defined, the arguments
        are returned as they were supplied.

        :param arguments: List of arguments for bound function as strings.
        :return: Mapped arguments.
        """
        if self.argument_mappings is None:
            return arguments

        return [f(a) for f, a in zip(self.argument_mappings, arguments)]

    def map_return_value(self, return_value):
        """
        Returns the mapped return_value of a processed request. If no return_mapping has been
        defined, the value is returned as is. If return_mapping is a static value, that value
        is returned, ignoring return_value completely.

        :param return_value: Value to map.
        :return: Mapped return value.
        """
        if callable(self.return_mapping):
            return self.return_mapping(return_value)

        if self.return_mapping is not None:
            return self.return_mapping

        return return_value


class CommandBase:
    """
    This is the common base class of :class:`Cmd` and :class:`Var`. The concept of commands for
    the stream adapter is based on connecting a callable object to a pattern that matches an
    inbound request.

    The type of pattern can be either an implementation of :class:`PatternMatcher`
    (regex or scanf format specification) or a plain string (which is treated as a regular
    expression).

    For free function and lambda expressions this is straightforward: the function object can
    simply be stored together with the pattern. Most often however, the callable
    is a method of the device or interface object - these do not exist when the commands are
    defined.

    This problem is solved by introducing a "bind"-step in :class:`StreamAdapter`. So instead
    of a function object, both :class:`Cmd` and :class:`Var` store the name of a member of device
    or interface. At "bind-time", this is translated into the correct callable.

    So instead of using :class:`Cmd` or :class:`Var` directly, both classes' :meth:`bind`-methods
    return an iterable of :class:`Func`-objects which can be used for processing requests.
    :class:`StreamAdapter` performs this bind-step when it's constructed. For details regarding
    the implementations, please see the corresponding classes.

    .. seealso::

        Please take a look at :class:`Cmd` for exposing callable objects or methods of
        device/interface and :class:`Var` for exposing attributes and properties.

        To see how argument_mappings, return_mapping and doc are applied, please look at
        :class:`Func`.

    :param func: Function to be called when pattern matches or member of device/interface.
    :param pattern: Pattern to match (:class:`PatternMatcher` or string).
    :param argument_mappings: Iterable with mapping functions from string to some type.
    :param return_mapping: Mapping function for return value of method.
    :param doc: Description of the command. If not supplied, the docstring is used.
    """

    def __init__(
        self, func, pattern, argument_mappings=None, return_mapping=None, doc=None
    ):
        super(CommandBase, self).__init__()

        self.func = func
        self.pattern = pattern
        self.argument_mappings = argument_mappings
        self.return_mapping = return_mapping
        self.doc = doc

    def bind(self, target):
        raise NotImplementedError("Binders need to implement the bind method.")


class Cmd(CommandBase):
    """
    This class is an implementation of :class:`CommandBase` that can expose a callable object
    or a named method of the device/interface controlled by :class:`StreamAdapter`.

    .. sourcecode:: Python

        def random():
            return 6

        SomeInterface(StreamInterface):
            commands = {
                Cmd(lambda: 4, pattern='^R$', doc='Returns a random number.'),
                Cmd('random', pattern='^RR$', doc='Better random number.'),
                Cmd(random, pattern='^RRR$', doc='The best random number.'),
            }

            def random(self):
                return 5

    The interface defined by the above example has three commands, ``R`` which calls a lambda
    function that always returns 4, ``RR``, which calls ``SomeInterface.random`` and returns 5 and
    lastly ``RRR`` which calls the free function defined above and returns the best random number.

    For a detailed explanation of requirements to the constructor arguments, please refer to the
    documentation of :class:`Func`, to which the arguments are forwarded.

    .. seealso ::

        :class:`Var` exposes attributes and properties of a device object. The documentation
        of :class:`Func` provides more information about the common constructor arguments.

    :param func: Function to be called when pattern matches or member of device/interface.
    :param pattern: Pattern to match (:class:`PatternMatcher` or string).
    :param argument_mappings: Iterable with mapping functions from string to some type.
    :param return_mapping: Mapping function for return value of method.
    :param doc: Description of the command. If not supplied, the docstring is used.
    """

    def __init__(
        self,
        func,
        pattern,
        argument_mappings=None,
        return_mapping=lambda x: None if x is None else str(x),
        doc=None,
    ):
        super(Cmd, self).__init__(func, pattern, argument_mappings, return_mapping, doc)

    def bind(self, target):
        method = self.func if callable(self.func) else getattr(target, self.func, None)

        if method is None:
            return None

        return [
            Func(
                method,
                self.pattern,
                self.argument_mappings,
                self.return_mapping,
                self.doc,
            )
        ]


class Var(CommandBase):
    r"""
    With this implementation of :class:`CommandBase` it's possible to expose plain data attributes
    or properties of device or interface. Getting and setting a value are separate procedures
    which both have their own pattern, read_pattern and write_pattern to match a command each.
    Please note that write_pattern has to have exactly one group defined to match a parameter.

    Due to this separation, parameters can be made read-only, write-only or read-write in the
    interface:

    .. sourcecode:: Python

        class SomeInterface(StreamInterface):
            commands = {
                Var('foo', read_pattern='^F$', write_pattern=r'^F=(\d+)$',
                    argument_mappings=(int,), doc='An integer attribute.'),
                Var('bar' read_pattern='^B$')
            }

            foo = 10

            @property
            def bar(self):
                return self.foo + 5

            @bar.setter
            def bar(self, new_bar):
                self.foo = new_bar - 5

    In the above example, the foo attribute can be read and written, it's automatically converted
    to an integer, while bar is a property that can only be read via the stream protocol.

    .. seealso::

        For exposing methods and free functions, there's the :class:`Cmd`-class.

    :param target_member: Attribute or property of device/interface to expose.
    :param read_pattern: Pattern to match for getter (:class:`PatternMatcher` or string).
    :param write_pattern: Pattern to match for setter (:class:`PatternMatcher` or string).
    :param argument_mappings: Iterable with mapping functions from string to some type,
                              only applied to setter.
    :param return_mapping: Mapping function for return value of method,
                           applied to getter and setter.
    :param doc: Description of the command. If not supplied, the docstring is used. For plain data
                attributes the only way to get docs is to supply this argument.
    """

    def __init__(
        self,
        target_member,
        read_pattern=None,
        write_pattern=None,
        argument_mappings=None,
        return_mapping=lambda x: None if x is None else str(x),
        doc=None,
    ):
        super(Var, self).__init__(
            target_member, None, argument_mappings, return_mapping, doc
        )

        self.target = None

        self.read_pattern = read_pattern
        self.write_pattern = write_pattern

    def bind(self, target):
        if self.func not in dir(target):
            return None

        funcs = []

        if self.read_pattern is not None:

            def getter():
                return getattr(target, self.func)

            # Copy docstring if target is a @property
            prop = getattr(type(target), self.func, None)
            if prop and inspect.isdatadescriptor(prop):
                getter.__doc__ = "Getter: " + inspect.getdoc(prop)

            funcs.append(
                Func(
                    getter,
                    self.read_pattern,
                    return_mapping=self.return_mapping,
                    doc=self.doc,
                )
            )

        if self.write_pattern is not None:

            def setter(new_value):
                setattr(target, self.func, new_value)

            # Copy docstring if target is a @property
            prop = getattr(type(target), self.func, None)
            if prop and inspect.isdatadescriptor(prop):
                setter.__doc__ = "Setter: " + inspect.getdoc(prop)

            funcs.append(
                Func(
                    setter,
                    self.write_pattern,
                    argument_mappings=self.argument_mappings,
                    return_mapping=self.return_mapping,
                    doc=self.doc,
                )
            )

        return funcs


class StreamAdapter(Adapter):
    """
    The StreamAdapter is the bridge between the Device Interface and the TCP Stream networking
    backend implementation.

    Available adapter options are:

     - bind_address: IP of network adapter to bind on (defaults to 0.0.0.0, or all adapters)
     - port: Port to listen on (defaults to 9999)
     - telnet_mode: When True, overrides in- and out-terminator for CRNL (defaults to False)

    :param options: Dictionary with options.
    """

    default_options = {"telnet_mode": False, "bind_address": "0.0.0.0", "port": 9999}

    def __init__(self, options=None):
        super(StreamAdapter, self).__init__(options)
        self._server = None

    @property
    def documentation(self):
        commands = [
            "{}:\n{}".format(
                cmd.matcher.pattern,
                format_doc_text(cmd.doc or inspect.getdoc(cmd.func) or ""),
            )
            for cmd in sorted(
                self.interface.bound_commands, key=lambda x: x.matcher.pattern
            )
        ]

        options = format_doc_text(
            "Listening on: {}\nPort: {}\nRequest terminator: {}\nReply terminator: {}".format(
                self._options.bind_address,
                self._options.port,
                repr(self.interface.in_terminator),
                repr(self.interface.out_terminator),
            )
        )

        return "\n\n".join(
            [
                inspect.getdoc(self.interface) or "",
                "Parameters\n==========",
                options,
                "Commands\n========",
            ]
            + commands
        )

    def start_server(self):
        """
        Starts the TCP stream server, binding to the configured host and port.
        Host and port are configured via the command line arguments.

        .. note:: The server does not process requests unless
                  :meth:`handle` is called in regular intervals.

        """
        if self._server is None:
            if self._options.telnet_mode:
                self.interface.in_terminator = "\r\n"
                self.interface.out_terminator = "\r\n"

            self._server = StreamServer(
                self._options.bind_address,
                self._options.port,
                self.interface,
                self.device_lock,
            )

    def stop_server(self):
        if self._server is not None:
            self._server.close()
            self._server = None

    @property
    def is_running(self):
        return self._server is not None

    def handle(self, cycle_delay=0.1):
        """
        Spend approximately ``cycle_delay`` seconds to process requests to the server.

        :param cycle_delay: S
        """
        asyncore.loop(cycle_delay, count=1)
        self._server.process(int(cycle_delay * 1000))


class StreamInterface(InterfaceBase):
    r"""
    This class is used to provide a TCP-stream based interface to a device.

    Many hardware devices use a protocol that is based on exchanging text with a client via
    a TCP stream. Sometimes RS232-based devices are also exposed this way via an adapter-box.
    This adapter makes it easy to mimic such a protocol.

    This class has the following attributes which may be overridden by subclasses:

     - protocol: What this interface is called for purposes of the -p commandline option.
       Defaults to "stream".
     - in_terminator, out_terminator: These define how lines are terminated when transferred
       to and from the device respectively. They are stripped/added automatically.
       Inverse of protocol file InTerminator and OutTerminator. The default is ``\\r``.
     - readtimeout: How many msec to wait for additional data between packets, once transmission
       of an incoming command has begun. Inverse of ReadTimeout in protocol files.
       Defaults to 100 (ms). Set to 0 to disable timeout completely.
     - commands: A list of :class:`~CommandBase`-objects that define mappings between protocol
       and device/interface methods/attributes.

    By default, commands are expressed as regular expressions, a simple example may look like this:

    .. sourcecode:: Python

        class SimpleDeviceStreamInterface(StreamInterface):
            commands = [
                Cmd('set_speed', r'^S=([0-9]+)$', argument_mappings=[int]),
                Cmd('get_speed', r'^S\?$')
                Var('speed', read_pattern=r'^V\?$', write_pattern=r'^V=([0-9]+)$')
            ]

            def set_speed(self, new_speed):
                self.device.speed = new_speed

            def get_speed(self):
                return self.device.speed

    The interface has two commands, ``S?`` to return the speed and ``S=10`` to set the speed
    to an integer value. It also exposes the same speed attribute as a variable, using auto-
    generated ``V?`` and ``V=10`` commands.

    As in the :class:`lewis.adapters.epics.EpicsInterface`, it does not matter whether the
    wrapped method is a part of the device or of the interface, this is handled automatically when
    a new device is assigned to the ``device``-property.

    In addition, the :meth:`handle_error`-method can be overridden. It is called when an exception
    is raised while handling commands.
    """
    protocol = "stream"

    in_terminator = "\r"
    out_terminator = "\r"

    readtimeout = 100

    commands = None

    def __init__(self):
        super(StreamInterface, self).__init__()
        self.bound_commands = None

    @property
    def adapter(self):
        return StreamAdapter

    def _bind_device(self):
        """
        This method implements ``_bind_device`` from :class:`~lewis.core.devices.InterfaceBase`.
        It binds Cmd and Var definitions to implementations in Interface and Device.
        """
        patterns = set()

        self.bound_commands = []

        for cmd in self.commands:
            bound = cmd.bind(self) or cmd.bind(self.device) or None

            if bound is None:
                raise RuntimeError(
                    "Unable to produce callable object for non-existing member '{}' "
                    "of device or interface.".format(cmd.func)
                )

            for bound_cmd in bound:
                pattern = bound_cmd.matcher.pattern
                if pattern in patterns:
                    raise RuntimeError(
                        "The regular expression {} is "
                        "associated with multiple commands.".format(pattern)
                    )

                patterns.add(pattern)

                self.bound_commands.append(bound_cmd)

    def handle_error(self, request, error):
        """
        Override this method to handle exceptions that are raised during command processing.
        The default implementation does nothing, so that any errors are silently ignored.

        :param request: The request that resulted in the error.
        :param error: The exception that was raised.
        """
