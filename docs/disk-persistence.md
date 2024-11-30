# Disk persistence

The `DatabaseStorageManager` implementation is responsible for managing the storage of objects in EleceDb using a
disk-based file structure. The file storage is divided into chunks, with each chunk corresponding to a file on disk.
When writing a new object to disk, if the maximum size of the file has been reached, a new chunk (or file) is created.

The concept of a `Page` exists here, where each page corresponds to a fixed-size segment within a specific chunk file,
allowing multiple objects to be stored. This design improves performance because not all I/O operations require first
reading a segment from disk, thanks to a page Least Recently Used (LRU) cache implemented in `PageBuffer`.

When writing to disk, empty segments within a chunk file are avoided by keeping track of these empty slots. A
`DbObjectSlotLocation` represents these empty locations and is tracked in `ReservedSlotTracer`. When an object is
removed
from disk, its chunk, position in the chunk, and length are saved so that the slot can be reused for an object of the
same size.

## Operations

### Store

There are three ways of storing an object:

- **Reuse a Free Slot**: Use a free slot previously occupied by a deleted object.
- **Use the Last Page**: Acquire the last page in the page buffer so that the object is stored after the last stored
  object, provided the object's size fits within the available space in the file.
- **Create a New Chunk**: If no space is left in the last page, create a new chunk file and store the object there.

Once the location for storing the new object is determined, the operation itself is straightforward, as it involves
writing the data bytes onto the file. This implies persisting the page where the object resides.

### Update

Updating an object on disk consists of modifying the underlying byte data array with the new row values and committing
the page. Since a page is just a smaller segment of a chunk file, the entire page can be overwritten when a modification
occurs.

### Select

To read an object from disk, the specific page where the object resides can be acquired from the page buffer, allowing
direct reading of the object bytes. If the read object is not marked as alive, it is treated as non-existent.

### Remove

Removing an object from disk actually only implies saving the position of the removed object and its length, so that it
can be reused by new objects that will be persisted.
This means that the remove operation essentially only marks an object as valid to be overwritten.
But in case something goes wrong, the objects have a flag that dictate whether they are still alive. So even if a
deleted object is read, we can always check for the flag to see if the row is valid.