# Comparison with other frameworks (DRAFT, NOT READY YET)

In addition to Deep Video Analytics currently there are two other open source video & visual data analytics being
actively developed. They are

- KWIVER Kitware Image and Video Exploitation and Retrieval (Developed by Kitware)
- Scanner (Developed by students at CMU & Stanford)

Deep Video Analytics differs substantially from both of them in following ways


## Assumption about frame decode and tight vs weak coupling between operations

Scanner is tightly coupled where once a frame is decoded, it's stored in memory for other tasks to use. KWIVER
represents individual frames in video as a series of messages that. In comparison, DVA uses a decode + store/cache,
where a subset of frames can be decoded and stored as jpegs for use later. If required
in DVA tasks can do "just in time" decode by using a segment and avoid storing additional data in form of JPEGs.


## Architecture & Processing jobs as code vs data

DVA uses a "client-server" pattern, where a client submits a processing task in form of DAG of operation represented in
JSON. Each processing job is thus "data" rather than "code". KWIVER and Scanner represent each processing pattern as
code. In case of scanner the each pattern is stored in a python file which combines "ops" to create a processing pipeline.
Similarly in case KWIVER each processing pattern is represented as


## Provenance & Data Structures for Regions, Tubes, VisualRelations, HyperVisualRelation, Indexes etc.

DVA provides strong provenance guarantees (e.g. each object has an associated event that tracks its creation) along with
schema for "derived" objects such as Regions (2D bounding box on an image), Tubes (Region extended temporally), Indexes
(set of vectors associated with Regions and Tubes), VisualRelations (which represent edges between derived objects).


## A front-end / admin interface


