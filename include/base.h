#include <cstdint>
#include <cstdlib>
#include <iostream>

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;

    // Release callback
    void (*release)(struct ArrowSchema*);
    // Opaque producer-specific data
    void* private_data;

};

struct ArrowArray {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;

    // Release callback
    void (*release)(struct ArrowArray*);
    // Opaque producer-specific data
    void* private_data;

};

#endif  // ARROW_C_DATA_INTERFACE

void print_schema(ArrowSchema* schema)
{
    std::cout << "===== Schema (" << std::hex << schema << std::dec << ") ======" << std::endl;
    std::cout << "Format: " << schema->format << std::endl;
    std::cout << "Name: " << schema->name << std::endl;
    std::cout << "Flags: " << schema->flags << " ";
    std::cout << ((schema->flags & ARROW_FLAG_NULLABLE) ? "Nullable" : "") << " ";
    std::cout << ((schema->flags & ARROW_FLAG_DICTIONARY_ORDERED) ? "Dict ordered" : "") << " ";
    std::cout << ((schema->flags & ARROW_FLAG_MAP_KEYS_SORTED) ? "Map keys sorted" : "") << std::endl;
    std::cout << "Num of children: " << schema->n_children << " [ ";
    for (int i=0; i<schema->n_children; i++) {
        std::cout << std::hex << schema->children[i] << std::dec << " ";
    }
    std::cout << "]" << std::endl << std::endl;

    for (int i=0; i<schema->n_children; i++) {
        print_schema(schema->children[i]);
    }
}

void print_array(ArrowArray* array)
{
    std::cout << "===== Array (" << std::hex << array << std::dec << ") ======" << std::endl;
    std::cout << "Length: " << array->length << std::endl;
    std::cout << "Null count: " << array->null_count << std::endl;
    std::cout << "Offset: " << array->offset << std::endl;
    std::cout << "Num of buffers: " << array->n_buffers << " [ ";
    for (int i=0; i<array->n_buffers; i++) {
        std::cout << std::hex << array->buffers[i] << std::dec << " ";
    }
    std::cout << "]" << std::endl;

    std::cout << "Num of children: " << array->n_children << " [ ";
    for (int i=0; i<array->n_children; i++) {
        std::cout << std::hex << array->children[i] << std::dec << " ";
    }
    std::cout << "]" << std::endl << std::endl;

    for (int i=0; i<array->n_children; i++) {
        print_array(array->children[i]);
    }
}

void arrow_release_array(ArrowArray* array)
{
    for (int i=0; i<array->n_buffers; i++) {
        free((void*) array->buffers[i]);
    }
    for (int i=0; i<array->n_children; i++) {
        array->children[i]->release(array->children[i]);
        free(array->children[i]);
    }

    free(array->buffers);
    free(array->children);

    array->release = nullptr;
}

void arrow_make_array(ArrowArray* array)
{
    array->length = 0;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 0;
    array->n_children = 0;
    array->buffers = (const void**) calloc(10, sizeof(void*));
    array->children = (ArrowArray**) calloc(10, sizeof(ArrowArray*));
    array->dictionary = nullptr;
    array->release = &arrow_release_array;
    array->private_data = nullptr;
}

template<typename T>
T* arrow_add_buffer(ArrowArray* array, int size)
{
    auto* buf = malloc(size * sizeof(T));

    array->buffers[array->n_buffers] = buf;
    array->n_buffers++;

    return (T*) buf;
}

ArrowArray* arrow_add_child(ArrowArray* parent)
{
    auto* child = (ArrowArray*) malloc(sizeof(ArrowArray));
    arrow_make_array(child);

    parent->children[parent->n_children] = child;
    parent->n_children++;

    return child;
}
