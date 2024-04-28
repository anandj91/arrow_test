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

    ArrowSchema() {}

    ArrowSchema(
        const char* format,
        const char* name,
        const char* metadata,
        int64_t flags,
        int64_t n_children,
        struct ArrowSchema** children,
        struct ArrowSchema* dictionary,
        void (*release)(struct ArrowSchema*),
        void* private_data) :
        format(format),
        name(name),
        metadata(metadata),
        flags(flags),
        n_children(n_children),
        children(children),
        dictionary(dictionary),
        release(release),
        private_data(private_data) {}

    ArrowSchema(const ArrowSchema& schema) :
        ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            schema.n_children,
            schema.children,
            schema.dictionary,
            schema.release,
            schema.private_data) {}

    void print_schema() {
        std::cout << "===== Schema ======" << std::endl;
        std::cout << "Format: " << this->format << std::endl;
        std::cout << "Name: " << this->name << std::endl;
        //std::cout << "Metadata: " << this->metadata << std::endl;
        std::cout << "Flags: " << this->flags << " ";
        std::cout << ((this->flags & ARROW_FLAG_NULLABLE) ? "Nullable" : "") << " ";
        std::cout << ((this->flags & ARROW_FLAG_DICTIONARY_ORDERED) ? "Dict ordered" : "") << " ";
        std::cout << ((this->flags & ARROW_FLAG_MAP_KEYS_SORTED) ? "Map keys sorted" : "") << std::endl;
        std::cout << "Num of children: " << this->n_children << std::endl;
        std::cout << std::endl;

        for (int i=0; i<this->n_children; i++) {
            this->children[i]->print_schema();
        }
    }
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

    ArrowArray() : ArrowArray(
        0,          /* length */
        0,          /* null_count */
        0,          /* offset */
        0,          /* n_buffers */
        0,          /* n_children */
        nullptr,    /* buffers */
        nullptr,    /* children */
        nullptr,    /* dictionary */
        nullptr,    /* release */
        nullptr     /* private_data */
    ) {
        this->buffers = (const void**) malloc(sizeof(char) * 10);
        this->children = (struct ArrowArray**) malloc(sizeof(ArrowArray) * 10);
        this->dictionary = nullptr;
        this->release = static_cast<void(*)(struct ArrowArray*)>(
            [] (struct ArrowArray* array) {
                for (int i=0; i<array->n_buffers; i++) {
                    free((void*) array->buffers[i]);
                }
                for (int i=0; i<array->n_children; i++) {
                    array->children[i]->release(array->children[i]);
                }

                free(array->buffers);
                free(array->children);

                array->release = nullptr;
            }
        );
        this->private_data = nullptr;
    }

    ArrowArray(const ArrowArray& array) : ArrowArray(
        array.length,
        array.null_count,
        array.offset,
        array.n_buffers,
        array.n_children,
        array.buffers,
        array.children,
        array.dictionary,
        array.release,
        array.private_data
    ) {}

    ArrowArray(
        int64_t length,
        int64_t null_count,
        int64_t offset,
        int64_t n_buffers,
        int64_t n_children,
        const void** buffers,
        struct ArrowArray** children,
        struct ArrowArray* dictionary,
        void (*release)(struct ArrowArray*),
        void* private_data) :
        length(length),
        null_count(null_count),
        offset(offset),
        n_buffers(n_buffers),
        n_children(n_children),
        buffers(buffers),
        children(children),
        dictionary(dictionary),
        release(release),
        private_data(private_data)
    {}

    void print_array() {
        std::cout << "===== Array ======" << std::endl;
        std::cout << "Length: " << this->length << std::endl;
        std::cout << "Null count: " << this->null_count << std::endl;
        std::cout << "Offset: " << this->offset << std::endl;
        std::cout << "Num of buffers: " << this->n_buffers << std::endl;
        std::cout << "Num of children: " << this->n_children << std::endl;
        std::cout << std::endl;

        for (int i=0; i<this->n_children; i++) {
            this->children[i]->print_array();
        }
    }

    template<typename T>
    T* add_buffer(int size) {
        auto buf = (void*) malloc(size);

        this->buffers[this->n_buffers] = buf;
        this->n_buffers++;

        return (T*) buf;
    }

};

#endif  // ARROW_C_DATA_INTERFACE
