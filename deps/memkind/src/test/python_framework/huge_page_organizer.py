#
#  Copyright (C) 2017 Intel Corporation.
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#  1. Redistributions of source code must retain the above copyright notice(s),
#     this list of conditions and the following disclaimer.
#  2. Redistributions in binary form must reproduce the above copyright notice(s),
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) ``AS IS'' AND ANY EXPRESS
#  OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
#  EVENT SHALL THE COPYRIGHT HOLDER(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
#  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
#  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
#  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
#  OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

from cmd_helper import CMD_helper
import os
import itertools

"""
Huge_page_organizer sets hugepages per NUMA node and restores initial setup of hugepages.
It writes and reads from the same file, so using Huge_page_organizer with parallel
execution may cause undefined behaviour.
"""
class Huge_page_organizer(object):

    cmd_helper = CMD_helper()
    path = "/sys/devices/system/node/node{0}/hugepages/hugepages-2048kB/nr_hugepages"
    restore_values = []

    def __restore(self):
        """Restore initial setup of hugepages."""
        for node_id, restore_value in enumerate(self.restore_values):
            self.__set_nr_hugepages(node_id, restore_value)

    def __get_nr_hugepages(self, node_id):
        """Return number of hugepages on given node or None if given node is not configured."""
        if not os.path.isfile(self.path.format(node_id)):
            return None
        with open(self.path.format(node_id), "r") as f:
            return int(f.read())

    def __set_nr_hugepages(self, node_id, nr_hugepages):
        """Set hugepages on given node to given number. Return True on success, False otherwise."""
        command = 'sh -c "echo {0} >> {1}"'.format(nr_hugepages, self.path.format(node_id))
        output, retcode = self.cmd_helper.execute_cmd(command, sudo=True)
        if retcode:
            return False
        return self.__get_nr_hugepages(node_id) is nr_hugepages

    def __init__(self, nr_hugepages_per_node):
        """Save current hugepages setup and set hugepages per NUMA node."""
        for node_id in itertools.count():
            nr_hugepages = self.__get_nr_hugepages(node_id)
            if nr_hugepages is None:
                break
            self.restore_values.append(nr_hugepages)
            retcode = self.__set_nr_hugepages(node_id, nr_hugepages_per_node)
            if not retcode:
               self.__restore()
            assert retcode, "Error: Could not set the requested amount of hugepages."

    def __del__(self):
        """Call __restore() function."""
        self.__restore()
