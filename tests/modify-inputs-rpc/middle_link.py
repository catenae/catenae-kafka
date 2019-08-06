#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0

    @rpc
    def add_input(self, context, topic):
        self.logger.log("invoked add")
        self.add_input_topic(topic)

    @rpc
    def remove_input(self, context, topic):
        self.logger.log("invoked remove")
        self.remove_input_topic(topic)


if __name__ == "__main__":
    MiddleLink().start()
