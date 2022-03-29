# core-sparkplugb-lib
A port of tahu for rust.

The hope is to make a pr of this into eclipse/tahu once it's finished. This is still wip and only published to get feedback from folks.

Sparkplug-b is an open and set definition for the transmission of iot data. It relies upon protobuf (protocol buffers), an immensely successfuly project from google to enforce a schema for messages. This has become a standard for ingestion and publication for many iot and manufacturing related tools and technologies.

Tahu as a library has a number of function which support and ease the use of the sparkplug-b protocol.

`core-sparkplugb-lib` is meant to add rust as a supported language to tahu.


# todo:
- Resolve some dissonance - tahu itself does not seem to be thread-safe we need it to be...
- This is currently a straight port, while the methods work they need to better tested and made to better follow rust best practices.