#include <string>
#include <iostream>
#include <sstream>
#include <functional>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <fstream>
#include <system_error>
#include <unordered_map>
#include <sys/stat.h>
#include <sys/mman.h>
#include <csignal>
#ifdef __linux__
#include <sys/prctl.h>
#endif

// needed for foreign interface
#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <unistd.h>

#include <limits>
#include <utility>

char* g_tmpdir;

uint8_t* foreign_call(const char* socket_filename, size_t mid, ...) __attribute__((sentinel));

// AUTO include statements start
#include "/home/dev/.local/share/morloc/src/morloc/plane/default/root-cpp/core.hpp"
#include "/home/dev/.local/share/morloc/src/morloc/plane/default/table-cpp/table.hpp"
// AUTO include statements end

// Proper linking of cppmorloc requires it be included AFTER the custom modules
#include "mlc_arrow.hpp"
#include "cppmorloc.hpp"

#define PROPAGATE_ERROR(errmsg) \
    if(errmsg != NULL) { \
      char errmsg_buffer[MAX_ERRMSG_SIZE] = { 0 }; \
      snprintf(errmsg_buffer, MAX_ERRMSG_SIZE, "Error C++ pool (%s:%d in %s):\n%s" , __FILE__, __LINE__, __func__, errmsg); \
      free(errmsg); \
      throw std::runtime_error(errmsg_buffer); \
    }

#define PROPAGATE_FAIL_PACKET(errmsg) \
    if(errmsg != NULL){ \
        uint8_t* fail_packet_ = make_fail_packet(errmsg); \
        free(errmsg); \
        return fail_packet_; \
    }


// AUTO serialization statements start
static Schema* mlc_schema_table[17];
void _init_schemas() {
    static const char* _schema_strs[] = {
        "<std::tuple<$1,$2>>t2<int>j<int>j",
        "<bool>b",
        "T:11x<int>j",
        "<std::vector<$2>>a:2<int>j",
        "<std::vector<$2>>a:3<int>j",
        "<std::vector<$2>>a:6<int>j",
        "<int>j",
        "T:21x<int>j1y<std::string>s",
        "T:11a<int>j",
        "T:11y<std::string>s",
        "<std::vector<$2>>a:3<double>f8",
        "T:31x<int>j1y<std::string>s1z<double>f8",
        "<std::vector<$2>>a:3<std::string>s",
        "<std::vector<$2>>a:5<int>j",
        "<std::vector<$2>>a:1<int>j",
        "<std::vector<$2>>a:3<bool>b",
        "<std::vector<$1>>a<std::string>s",
    };
    for (int i = 0; i < 17; i++)
        mlc_schema_table[i] = parse_schema_cpp(_schema_strs[i]);
}
// AUTO serialization statements end



std::string interweave_strings(const std::vector<std::string>& first, const std::vector<std::string>& second)
{
    // Validate sizes - errors here indicate a bug in the morloc compiler
    if (first.size() != second.size() + 1) {
        throw std::invalid_argument("First list must have exactly 1 more element than second list");
    }

    // Pre-calculate total size to avoid reallocations
    size_t total_size = 0;
    for (const auto& s : first) total_size += s.size();
    for (const auto& s : second) total_size += s.size();

    std::string result;
    result.reserve(total_size);

    // Interweave the strings
    for (size_t i = 0; i < second.size(); ++i) {
        result += first[i];
        result += second[i];
    }
    result += first.back();  // Append the final element from first list

    return result;
}

// Thread-local list of SHM pointers allocated by _put_value.
// Freed after foreign_call returns (args consumed) or at next dispatch start
// (result consumed by caller in the synchronous call that returned it).
struct ShmEntry { absptr_t ptr; Schema* schema; };
thread_local std::vector<ShmEntry> _shm_tracker;

static void _flush_shm_tracker() {
    for (auto& e : _shm_tracker) {
        char* err = NULL;
        // Only do recursive sub-freeing if we have a schema and this is
        // the last reference. NULL schema entries (from foreign_call result
        // tracking) just decrement the refcount.
        block_header_t* blk = (block_header_t*)((char*)e.ptr - sizeof(block_header_t));
        if (e.schema && blk->reference_count <= 1) {
            shfree_by_schema(e.ptr, e.schema, &err);
            if (err) { free(err); err = NULL; }
        }
        shfree(e.ptr, &err);
        if (err) { free(err); }
    }
    _shm_tracker.clear();
}

// Transforms a serialized value into a message ready for the socket
template <typename T>
uint8_t* _put_value(const T& value, Schema* schema) {

    if constexpr (std::is_same_v<T, mlc::ArrowTable>) {
        // Arrow export: move table data into SHM, build packet.
        // const_cast is safe here: the value is always a temporary from
        // a manifold call, never a truly const object.
        mlc::ArrowTable& tbl = const_cast<mlc::ArrowTable&>(value);
        relptr_t relptr = tbl.move_to_shm();

        uint8_t* packet = make_arrow_data_packet(relptr, schema);
        if (!packet) { throw std::runtime_error("Failed to create arrow data packet"); }

        char* err = nullptr;
        void* shm_ptr = rel2abs(relptr, &err);
        if (err) { free(err); }
        if (shm_ptr) { _shm_tracker.push_back({(absptr_t)shm_ptr, nullptr}); }
        return packet;
    } else {
        // Arrow dispatch: schema marker `T` (MORLOC_TABLE) routes through
        // mlc::ArrowTable. The legacy `<arrow>` hint has been retired.
        if (schema->type == MORLOC_TABLE) {
            throw std::runtime_error("Table schema but C++ type is not mlc::ArrowTable");
        }

        void* voidstar = nullptr;
        try {
            voidstar = toAnything(schema, value);
            relptr_t relptr = abs2rel_cpp(voidstar);

            char* errmsg = nullptr;
            uint8_t* packet = make_data_packet_auto(voidstar, relptr, schema, &errmsg);
            if (errmsg) {
                shfree_cpp(voidstar);
                PROPAGATE_ERROR(errmsg);
            }

            const morloc_packet_header_t* hdr = (const morloc_packet_header_t*)packet;
            if (hdr->command.data.source == PACKET_SOURCE_RPTR) {
                // SHM referenced by packet -- track for deferred cleanup
                _shm_tracker.push_back({(absptr_t)voidstar, schema});
            } else {
                // Data inlined in packet -- free SHM immediately
                char* free_err = NULL;
                shfree_by_schema((absptr_t)voidstar, schema, &free_err);
                if (free_err) { free(free_err); free_err = NULL; }
                shfree((absptr_t)voidstar, &free_err);
                if (free_err) { free(free_err); }
            }
            return packet;
        } catch (...) {
            if (voidstar) shfree_cpp(voidstar);
            throw;
        }
    }
}


// Use a key to retrieve a value
template <typename T>
T _get_value(const uint8_t* packet, Schema* schema){
    const morloc_packet_header_t* header = (const morloc_packet_header_t*)packet;
    uint8_t source = header->command.data.source;
    uint8_t format = header->command.data.format;

    if constexpr (std::is_same_v<T, mlc::ArrowTable>) {
        // Arrow import: packet -> arrow_from_shm -> ArrowTable
        char* errmsg = nullptr;
        uint8_t* raw = get_morloc_data_packet_value(packet, schema, &errmsg);
        if (errmsg) { PROPAGATE_ERROR(errmsg); }

        const arrow_shm_header_t* hdr = (const arrow_shm_header_t*)raw;
        struct ArrowSchema as;
        struct ArrowArray aa;
        char* aerr = nullptr;
        arrow_from_shm(hdr, &as, &aa, &aerr);
        if (aerr) { PROPAGATE_ERROR(aerr); }

        char* ierr = nullptr;
        shincref((absptr_t)raw, &ierr);
        if (ierr) { free(ierr); }
        _shm_tracker.push_back({(absptr_t)raw, nullptr});

        return mlc::ArrowTable(std::move(as), std::move(aa));
    } else {
        if (format == PACKET_FORMAT_ARROW) {
            throw std::runtime_error("Arrow data but C++ type is not mlc::ArrowTable");
        }

        // Fast path: inline voidstar -- read directly from packet, no SHM needed
        if (source == PACKET_SOURCE_MESG && format == PACKET_FORMAT_VOIDSTAR) {
            const uint8_t* payload = packet + sizeof(morloc_packet_header_t) + header->offset;
            T* dummy = nullptr;
            return fromAnything(schema, (const void*)payload, dummy, (const void*)payload);
        }

        // SHM paths (RPTR or MESG+MSGPACK): existing logic
        bool is_rptr = (source == PACKET_SOURCE_RPTR);

        char* errmsg = NULL;
        uint8_t* voidstar = get_morloc_data_packet_value(packet, schema, &errmsg);
        if(errmsg != NULL) {
            PROPAGATE_ERROR(errmsg)
        }

        // For RPTR data, increment refcount so the owner's tracker flush
        // won't destroy data we may still need (e.g. forwarded packets).
        if (is_rptr) {
            char* incref_err = NULL;
            shincref((absptr_t)voidstar, &incref_err);
            if (incref_err) { free(incref_err); }
            _shm_tracker.push_back({(absptr_t)voidstar, schema});
        }

        T* dummy = nullptr;
        return fromAnything(schema, (void*)voidstar, dummy);
    }
}


// Hash a value, returning a 16-char hex string
template <typename T>
std::string _mlc_hash(const T& value, Schema* schema) {
    void* voidstar = toAnything(schema, value);
    char* errmsg = NULL;
    char* hex = mlc_hash(voidstar, schema, &errmsg);
    shfree_cpp(voidstar);
    if (errmsg != NULL) {
        PROPAGATE_ERROR(errmsg)
    }
    std::string result(hex);
    free(hex);
    return result;
}

// Save a value to file in msgpack format
template <typename T>
void _mlc_save(const T& value, Schema* schema, const std::string& path) {
    void* voidstar = toAnything(schema, value);
    char* errmsg = NULL;
    mlc_save(voidstar, schema, path.c_str(), &errmsg);
    shfree_cpp(voidstar);
    if (errmsg != NULL) {
        PROPAGATE_ERROR(errmsg)
    }
}

// Save a value to file in flat voidstar binary format
template <typename T>
void _mlc_save_voidstar(const T& value, Schema* schema, const std::string& path) {
    void* voidstar = toAnything(schema, value);
    char* errmsg = NULL;
    mlc_save_voidstar(voidstar, schema, path.c_str(), &errmsg);
    shfree_cpp(voidstar);
    if (errmsg != NULL) {
        PROPAGATE_ERROR(errmsg)
    }
}

// Save a value to file in JSON format
template <typename T>
void _mlc_save_json(const T& value, Schema* schema, const std::string& path) {
    void* voidstar = toAnything(schema, value);
    char* errmsg = NULL;
    mlc_save_json(voidstar, schema, path.c_str(), &errmsg);
    shfree_cpp(voidstar);
    if (errmsg != NULL) {
        PROPAGATE_ERROR(errmsg)
    }
}

// Serialize a value to a JSON string
template <typename T>
std::string _mlc_show(const T& value, Schema* schema) {
    void* voidstar = toAnything(schema, value);
    char* errmsg = NULL;
    char* json = mlc_show(voidstar, schema, &errmsg);
    shfree_cpp(voidstar);
    if (errmsg != NULL) {
        PROPAGATE_ERROR(errmsg)
    }
    std::string result(json);
    free(json);
    return result;
}

// Deserialize a JSON string to a typed value
// Returns std::nullopt on parse failure
template <typename T>
std::optional<T> _mlc_read(Schema* schema, const std::string& json_str) {
    char* errmsg = NULL;
    void* voidstar = mlc_read(json_str.c_str(), schema, &errmsg);
    if (errmsg != NULL) {
        PROPAGATE_ERROR(errmsg)
    }
    if (voidstar == NULL) {
        return std::nullopt;
    }
    T* dummy = nullptr;
    T result = fromAnything(schema, voidstar, dummy);
    shfree_cpp(voidstar);
    return result;
}

// Load a value from file, auto-detecting format
// Returns std::nullopt if file does not exist
template <typename T>
std::optional<T> _mlc_load(Schema* schema, const std::string& path) {
    char* errmsg = NULL;
    void* voidstar = mlc_load(path.c_str(), schema, &errmsg);
    if (errmsg != NULL) {
        PROPAGATE_ERROR(errmsg)
    }
    if (voidstar == NULL) {
        return std::nullopt;
    }
    T* dummy = nullptr;
    T result = fromAnything(schema, voidstar, dummy);
    shfree_cpp(voidstar);
    return result;
}

uint8_t* foreign_call(const char* socket_filename, size_t mid, ...) {
    char* errmsg = NULL;
    va_list args;
    size_t nargs = 0;

    char socket_path[128];
    snprintf(socket_path, sizeof(socket_path), "%s/%s", g_tmpdir, socket_filename);

    // Count arguments (must be NULL-terminated)
    va_start(args, mid);
    while (va_arg(args, uint8_t*) != NULL) nargs++;
    va_end(args);

    // Allocate and populate args array
    const uint8_t** args_array = (const uint8_t**)malloc((nargs + 1) * sizeof(uint8_t*));
    if (!args_array) throw std::runtime_error("malloc failed in foreign_call");

    va_start(args, mid);
    for (size_t i = 0; i < nargs; i++) {
        args_array[i] = va_arg(args, uint8_t*);
    }
    args_array[nargs] = NULL;  // Sentinel
    va_end(args);

    // Original logic with variadic args converted to array
    uint8_t* packet = make_morloc_local_call_packet((uint32_t)mid, args_array, nargs, &errmsg);
    if (errmsg != NULL) {
        free(args_array);
        PROPAGATE_ERROR(errmsg)
    }

    pool_mark_busy();
    uint8_t* result = send_and_receive_over_socket(socket_path, packet, &errmsg);
    pool_mark_idle();

    free(packet);

    if (errmsg != NULL) {
        free(args_array);
        PROPAGATE_ERROR(errmsg)
    }

    // Incref the result's SHM so the callee's tracker flush won't destroy
    // data we may still need (e.g. forwarded result packets).
    {
        const morloc_packet_header_t* res_header = (const morloc_packet_header_t*)result;
        if (res_header->command.data.source == PACKET_SOURCE_RPTR) {
            size_t relptr = *(size_t*)(result + res_header->offset + sizeof(morloc_packet_header_t));
            char* resolve_err = NULL;
            void* res_voidstar = rel2abs(relptr, &resolve_err);
            if (resolve_err) { free(resolve_err); resolve_err = NULL; }
            if (res_voidstar) {
                char* incref_err = NULL;
                shincref((absptr_t)res_voidstar, &incref_err);
                if (incref_err) { free(incref_err); }
                _shm_tracker.push_back({(absptr_t)res_voidstar, nullptr});
            }
        }
    }

    free(args_array);
    return result;
}



// AUTO signatures statements start
uint8_t* m1();
uint8_t* m2725();
uint8_t* m2871();
uint8_t* m2879(const uint8_t* s3);
uint8_t* m2884(const uint8_t* s0);
uint8_t* m2893(const uint8_t* s3);
uint8_t* m2905(const uint8_t* s1);
uint8_t* m2912(const uint8_t* s3);
uint8_t* m2926(const uint8_t* s2);
uint8_t* m2978(const uint8_t* s4);
uint8_t* m2987();
uint8_t* m3003();
uint8_t* m3017();
uint8_t* m3031();
uint8_t* m3039();
uint8_t* m3173();
uint8_t* m3180();
uint8_t* m3187();
uint8_t* m3254();
uint8_t* m3259();
uint8_t* m3265();
uint8_t* m3361();
uint8_t* m3368();
uint8_t* m3376();
uint8_t* m3383();
uint8_t* m3389();
uint8_t* m3395();
uint8_t* m3527();
uint8_t* m3532();
uint8_t* m3540();
uint8_t* m3546();
uint8_t* m3552();
uint8_t* m3559();
uint8_t* m3690(const uint8_t* s6);
uint8_t* m3695(const uint8_t* s6);
uint8_t* m3707();
uint8_t* m3714();
uint8_t* m3722();
uint8_t* m3729();
uint8_t* m3887();
uint8_t* m3899();
uint8_t* m3907();
uint8_t* m4022();
uint8_t* m4031();
uint8_t* m4039(const uint8_t* s9);
uint8_t* m4049();
uint8_t* m4057();
uint8_t* m4066(const uint8_t* s9);
uint8_t* m4073();
uint8_t* m4079();
uint8_t* m4178(const uint8_t* s10);
uint8_t* m4188(const uint8_t* s12);
uint8_t* m4196(const uint8_t* s14);
uint8_t* m4207();
uint8_t* m4212();
uint8_t* m4220(const uint8_t* s11);
uint8_t* m4223(const uint8_t* s16);
uint8_t* m4230(const uint8_t* s13);
uint8_t* m4319(const uint8_t* s17);
uint8_t* m4325();
uint8_t* m4333(const uint8_t* s18);
uint8_t* m4339(const uint8_t* s21);
uint8_t* m4348(const uint8_t* s19);
uint8_t* m4354(const uint8_t* s22);
uint8_t* m4361(const uint8_t* s20);
uint8_t* m4367(const uint8_t* s23);
uint8_t* m4474(const uint8_t* s24);
uint8_t* m4480();
uint8_t* m4488(const uint8_t* s25);
uint8_t* m4502(const uint8_t* s26);
uint8_t* m4508(const uint8_t* s27);
uint8_t* m4515(const uint8_t* s25);
uint8_t* m4624();
uint8_t* m4632(const uint8_t* s34);
uint8_t* m4645(const uint8_t* s34);
uint8_t* m4659(const uint8_t* s34);
uint8_t* m4664(const uint8_t* s28);
uint8_t* m4673(const uint8_t* s34);
uint8_t* m4678(const uint8_t* s31);
uint8_t* m4687(const uint8_t* s34);
uint8_t* m4692(const uint8_t* s33);
uint8_t* m4701(const uint8_t* s34);
uint8_t* m4714(const uint8_t* s34);
uint8_t* m4727(const uint8_t* s34);
uint8_t* m4732(const uint8_t* s29);
uint8_t* m4741(const uint8_t* s34);
uint8_t* m4746(const uint8_t* s32);
uint8_t* m4755(const uint8_t* s34);
uint8_t* m4760(const uint8_t* s32);
uint8_t* m4769(const uint8_t* s34);
uint8_t* m4774(const uint8_t* s30);
uint8_t* m4783(const uint8_t* s34);
uint8_t* m4794(const uint8_t* s34);
uint8_t* m4907();
uint8_t* m4917();
uint8_t* m4981();
uint8_t* m4985();
uint8_t* m4993();
uint8_t* m5000();
uint8_t* m5008();
uint8_t* m5017();
// AUTO signatures statements end



// AUTO manifolds statements start
std::tuple<int,int> m2825()
{
    try
    {
        uint8_t* s1 = foreign_call("pipe-py", 2825, NULL);
        return(_get_value<std::tuple<int,int>>(s1, mlc_schema_table[0]));
    } catch (...)
      {
          throw;
      }
}

int m2832()
{
    try
    {
        int n2 = std::get<0>(m2825());
        return(n2);
    } catch (...)
      {
          throw;
      }
}

uint8_t* m1()
{
    try
    {
        bool n3 = morloc_eq(0, m2832());
        return(_put_value(n3, mlc_schema_table[1]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2725()
{
    try
    {
        std::tuple<int,int> n1 = std::make_tuple(0, 0);
        return(_put_value(n1, mlc_schema_table[0]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2871()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2879(const uint8_t* s3)
{
    try
    {
        mlc::ArrowTable n3 = _get_value<mlc::ArrowTable>(s3, mlc_schema_table[2]);
        mlc::ArrowTable n4 = morloc_sliceRows(0, 2, n3);
        return(_put_value(n4, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2884(const uint8_t* s0)
{
    try
    {
        std::vector<int> n0 = _get_value<std::vector<int>>(s0, mlc_schema_table[3]);
        mlc::ArrowTable n1 = morloc_asCol(std::string("x"), n0);
        return(_put_value(n1, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2893(const uint8_t* s3)
{
    try
    {
        mlc::ArrowTable n3 = _get_value<mlc::ArrowTable>(s3, mlc_schema_table[2]);
        int n6 = morloc_nrow(n3);
        int n4 = morloc_nrow(n3);
        int n5 = (n4 - 2);
        mlc::ArrowTable n7 = morloc_sliceRows(n5, n6, n3);
        return(_put_value(n7, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2905(const uint8_t* s1)
{
    try
    {
        std::vector<int> n1 = _get_value<std::vector<int>>(s1, mlc_schema_table[3]);
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2912(const uint8_t* s3)
{
    try
    {
        mlc::ArrowTable n3 = _get_value<mlc::ArrowTable>(s3, mlc_schema_table[2]);
        int n4 = morloc_nrow(n3);
        int n5 = (n4 - 1);
        std::vector<int> n6 = morloc_range(0, n5);
        std::vector<int> n7 = morloc_reverse(n6);
        mlc::ArrowTable n8 = morloc_pickRows(n7, n3);
        return(_put_value(n8, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2926(const uint8_t* s2)
{
    try
    {
        std::vector<int> n2 = _get_value<std::vector<int>>(s2, mlc_schema_table[4]);
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        return(_put_value(n3, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2978(const uint8_t* s4)
{
    try
    {
        std::vector<int> n4 = _get_value<std::vector<int>>(s4, mlc_schema_table[5]);
        mlc::ArrowTable n5 = morloc_asCol(std::string("x"), n4);
        return(_put_value(n5, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m2987()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        mlc::ArrowTable n5 = morloc_rbind(n2, n4);
        int n6 = morloc_nrow(n5);
        return(_put_value(n6, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3003()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        mlc::ArrowTable n5 = morloc_rbind(n2, n4);
        return(_put_value(n5, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3017()
{
    try
    {
        std::vector<std::string> n3 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n4 = morloc_asCol(std::string("y"), n3);
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        mlc::ArrowTable n5 = morloc_cbind(n2, n4);
        int n6 = morloc_ncol(n5);
        return(_put_value(n6, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3031()
{
    try
    {
        std::vector<std::string> n3 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n4 = morloc_asCol(std::string("y"), n3);
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        mlc::ArrowTable n5 = morloc_cbind(n2, n4);
        return(_put_value(n5, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3039()
{
    try
    {
        std::vector<int> n2 = {0,1,2};
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n4 = morloc_setCol(std::string("y"), n1, n3);
        return(_put_value(n4, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3173()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        mlc::ArrowTable n3 = morloc_renameCol( std::string("x")
        , std::string("a")
        , n2 );
        return(_put_value(n3, mlc_schema_table[8]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3180()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("a"), n1);
        return(_put_value(n2, mlc_schema_table[8]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3187()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        mlc::ArrowTable n3 = morloc_renameCol( std::string("x")
        , std::string("a")
        , n2 );
        std::vector<int> n4 = morloc_getCol(std::string("a"), n3);
        return(_put_value(n4, mlc_schema_table[4]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3254()
{
    try
    {
        std::vector<int> n4 = {0,1,2};
        mlc::ArrowTable n5 = morloc_asCol(std::string("x"), n4);
        std::vector<std::string> n3 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n6 = morloc_setCol(std::string("y"), n3, n5);
        std::vector<double> n2 = {0.0,0.5,1.0};
        mlc::ArrowTable n7 = morloc_setCol(std::string("z"), n2, n6);
        std::vector<std::string> n1 = {std::string("x")};
        mlc::ArrowTable n8 = morloc_selectColsDyn(n1, n7);
        return(_put_value(n8, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3259()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3265()
{
    try
    {
        std::vector<int> n4 = {0,1,2};
        mlc::ArrowTable n5 = morloc_asCol(std::string("x"), n4);
        std::vector<std::string> n3 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n6 = morloc_setCol(std::string("y"), n3, n5);
        std::vector<double> n2 = {0.0,0.5,1.0};
        mlc::ArrowTable n7 = morloc_setCol(std::string("z"), n2, n6);
        std::vector<std::string> n1 = {std::string("y"),std::string("z")};
        mlc::ArrowTable n8 = morloc_selectColsDyn(n1, n7);
        int n9 = morloc_ncol(n8);
        return(_put_value(n9, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3361()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<std::string> n1 = {std::string("x")};
        mlc::ArrowTable n6 = morloc_selectCols(n1, n5);
        return(_put_value(n6, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3368()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3376()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<std::string> n1 = {std::string("y")};
        mlc::ArrowTable n6 = morloc_selectCols(n1, n5);
        return(_put_value(n6, mlc_schema_table[9]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3383()
{
    try
    {
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n2 = morloc_asCol(std::string("y"), n1);
        return(_put_value(n2, mlc_schema_table[9]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3389()
{
    try
    {
        std::vector<int> n4 = {0,1,2};
        mlc::ArrowTable n5 = morloc_asCol(std::string("x"), n4);
        std::vector<std::string> n3 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n6 = morloc_setCol(std::string("y"), n3, n5);
        std::vector<double> n2 = {0.0,0.5,1.0};
        mlc::ArrowTable n7 = morloc_setCol(std::string("z"), n2, n6);
        std::vector<std::string> n1 = {std::string("x"),std::string("y")};
        mlc::ArrowTable n8 = morloc_selectCols(n1, n7);
        return(_put_value(n8, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3395()
{
    try
    {
        std::vector<int> n2 = {0,1,2};
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n4 = morloc_setCol(std::string("y"), n1, n3);
        return(_put_value(n4, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3527()
{
    try
    {
        std::vector<int> n4 = {0,1,2};
        mlc::ArrowTable n5 = morloc_asCol(std::string("x"), n4);
        std::vector<std::string> n3 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n6 = morloc_setCol(std::string("y"), n3, n5);
        std::vector<double> n2 = {0.0,0.5,1.0};
        mlc::ArrowTable n7 = morloc_setCol(std::string("z"), n2, n6);
        std::vector<std::string> n1 = {std::string("z")};
        mlc::ArrowTable n8 = morloc_dropCols(n1, n7);
        return(_put_value(n8, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3532()
{
    try
    {
        std::vector<int> n2 = {0,1,2};
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n4 = morloc_setCol(std::string("y"), n1, n3);
        return(_put_value(n4, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3540()
{
    try
    {
        std::vector<int> n4 = {0,1,2};
        mlc::ArrowTable n5 = morloc_asCol(std::string("x"), n4);
        std::vector<std::string> n3 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n6 = morloc_setCol(std::string("y"), n3, n5);
        std::vector<double> n2 = {0.0,0.5,1.0};
        mlc::ArrowTable n7 = morloc_setCol(std::string("z"), n2, n6);
        std::vector<std::string> n1 = {std::string("x"),std::string("z")};
        mlc::ArrowTable n8 = morloc_dropCols(n1, n7);
        return(_put_value(n8, mlc_schema_table[9]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3546()
{
    try
    {
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n2 = morloc_asCol(std::string("y"), n1);
        return(_put_value(n2, mlc_schema_table[9]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3552()
{
    try
    {
        std::vector<int> n2 = {0,1,2};
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        std::vector<std::string> n1 = {std::string("absent")};
        mlc::ArrowTable n4 = morloc_dropCols(n1, n3);
        return(_put_value(n4, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3559()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3690(const uint8_t* s6)
{
    try
    {
        std::vector<double> n6 = _get_value<std::vector<double>>(s6, mlc_schema_table[10]);
        std::vector<int> n9 = {0,1,2};
        mlc::ArrowTable n10 = morloc_asCol(std::string("x"), n9);
        std::vector<std::string> n8 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n11 = morloc_setCol(std::string("y"), n8, n10);
        std::vector<double> n7 = {0.0,0.5,1.0};
        mlc::ArrowTable n12 = morloc_setCol(std::string("z"), n7, n11);
        mlc::ArrowTable n13 = morloc_setCol(std::string("z"), n6, n12);
        return(_put_value(n13, mlc_schema_table[11]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3695(const uint8_t* s6)
{
    try
    {
        std::vector<double> n6 = _get_value<std::vector<double>>(s6, mlc_schema_table[10]);
        std::vector<int> n8 = {0,1,2};
        mlc::ArrowTable n9 = morloc_asCol(std::string("x"), n8);
        std::vector<std::string> n7 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n10 = morloc_setCol(std::string("y"), n7, n9);
        mlc::ArrowTable n11 = morloc_setCol(std::string("z"), n6, n10);
        return(_put_value(n11, mlc_schema_table[11]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3707()
{
    try
    {
        std::vector<int> n2 = {0,1,2};
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n4 = morloc_setCol(std::string("y"), n1, n3);
        return(_put_value(n4, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3714()
{
    try
    {
        std::vector<int> n2 = {0,1,2};
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n4 = morloc_setCol(std::string("y"), n1, n3);
        return(_put_value(n4, mlc_schema_table[7]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3722()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<double> n1 = {0.0,0.5,1.0};
        mlc::ArrowTable n6 = morloc_setCol(std::string("z"), n1, n5);
        return(_put_value(n6, mlc_schema_table[11]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3729()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<double> n1 = {0.0,0.5,1.0};
        mlc::ArrowTable n6 = morloc_setCol(std::string("z"), n1, n5);
        return(_put_value(n6, mlc_schema_table[11]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3887()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        std::vector<int> n3 = morloc_getCol(std::string("x"), n2);
        return(_put_value(n3, mlc_schema_table[4]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3899()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<double> n1 = {0.0,0.5,1.0};
        mlc::ArrowTable n6 = morloc_setCol(std::string("z"), n1, n5);
        std::vector<std::string> n7 = morloc_getCol(std::string("y"), n6);
        return(_put_value(n7, mlc_schema_table[12]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m3907()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<double> n1 = {0.0,0.5,1.0};
        mlc::ArrowTable n6 = morloc_setCol(std::string("z"), n1, n5);
        std::vector<double> n7 = morloc_getCol(std::string("z"), n6);
        return(_put_value(n7, mlc_schema_table[10]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4022()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::tuple<std::string,bool> n1 = std::make_tuple( std::string("x")
        , true );
        std::vector<std::tuple<std::string,bool>> n2 = {n1};
        mlc::ArrowTable n5 = morloc_sortRows(n2, n4);
        return(_put_value(n5, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4031()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4039(const uint8_t* s9)
{
    try
    {
        std::vector<int> n9 = _get_value<std::vector<int>>(s9, mlc_schema_table[4]);
        mlc::ArrowTable n12 = morloc_asCol(std::string("x"), n9);
        std::tuple<std::string,bool> n10 = std::make_tuple( std::string("x")
        , true );
        std::vector<std::tuple<std::string,bool>> n11 = {n10};
        mlc::ArrowTable n13 = morloc_sortRows(n11, n12);
        return(_put_value(n13, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4049()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4057()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::tuple<std::string,bool> n1 = std::make_tuple( std::string("x")
        , false );
        std::vector<std::tuple<std::string,bool>> n2 = {n1};
        mlc::ArrowTable n5 = morloc_sortRows(n2, n4);
        return(_put_value(n5, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4066(const uint8_t* s9)
{
    try
    {
        std::vector<int> n9 = _get_value<std::vector<int>>(s9, mlc_schema_table[4]);
        mlc::ArrowTable n10 = morloc_asCol(std::string("x"), n9);
        return(_put_value(n10, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4073()
{
    try
    {
        std::vector<int> n2 = {0,1,2};
        mlc::ArrowTable n3 = morloc_asCol(std::string("x"), n2);
        std::vector<std::tuple<std::string,bool>> n1 = {};
        mlc::ArrowTable n4 = morloc_sortRows(n1, n3);
        return(_put_value(n4, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4079()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4178(const uint8_t* s10)
{
    try
    {
        std::vector<int> n10 = _get_value<std::vector<int>>(s10, mlc_schema_table[4]);
        mlc::ArrowTable n11 = morloc_asCol(std::string("x"), n10);
        return(_put_value(n11, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4188(const uint8_t* s12)
{
    try
    {
        std::vector<int> n12 = _get_value<std::vector<int>>(s12, mlc_schema_table[13]);
        mlc::ArrowTable n13 = morloc_asCol(std::string("x"), n12);
        return(_put_value(n13, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4196(const uint8_t* s14)
{
    try
    {
        std::vector<int> n14 = _get_value<std::vector<int>>(s14, mlc_schema_table[4]);
        mlc::ArrowTable n15 = morloc_asCol(std::string("x"), n14);
        return(_put_value(n15, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4207()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        mlc::ArrowTable n3 = morloc_distinctRows(n2);
        return(_put_value(n3, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4212()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4220(const uint8_t* s11)
{
    try
    {
        mlc::ArrowTable n11 = _get_value<mlc::ArrowTable>(s11, mlc_schema_table[2]);
        mlc::ArrowTable n12 = morloc_distinctRows(n11);
        return(_put_value(n12, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4223(const uint8_t* s16)
{
    try
    {
        std::vector<int> n16 = _get_value<std::vector<int>>(s16, mlc_schema_table[14]);
        mlc::ArrowTable n17 = morloc_asCol(std::string("x"), n16);
        return(_put_value(n17, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4230(const uint8_t* s13)
{
    try
    {
        mlc::ArrowTable n13 = _get_value<mlc::ArrowTable>(s13, mlc_schema_table[2]);
        mlc::ArrowTable n14 = morloc_distinctRows(n13);
        return(_put_value(n14, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4319(const uint8_t* s17)
{
    try
    {
        std::vector<int> n17 = _get_value<std::vector<int>>(s17, mlc_schema_table[4]);
        std::vector<int> n18 = {0,1,2};
        mlc::ArrowTable n19 = morloc_asCol(std::string("x"), n18);
        mlc::ArrowTable n20 = morloc_pickRows(n17, n19);
        return(_put_value(n20, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4325()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4333(const uint8_t* s18)
{
    try
    {
        std::vector<int> n18 = _get_value<std::vector<int>>(s18, mlc_schema_table[4]);
        std::vector<int> n19 = {0,1,2};
        mlc::ArrowTable n20 = morloc_asCol(std::string("x"), n19);
        mlc::ArrowTable n21 = morloc_pickRows(n18, n20);
        return(_put_value(n21, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4339(const uint8_t* s21)
{
    try
    {
        std::vector<int> n21 = _get_value<std::vector<int>>(s21, mlc_schema_table[4]);
        mlc::ArrowTable n22 = morloc_asCol(std::string("x"), n21);
        return(_put_value(n22, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4348(const uint8_t* s19)
{
    try
    {
        std::vector<int> n19 = _get_value<std::vector<int>>(s19, mlc_schema_table[5]);
        std::vector<int> n20 = {0,1,2};
        mlc::ArrowTable n21 = morloc_asCol(std::string("x"), n20);
        mlc::ArrowTable n22 = morloc_pickRows(n19, n21);
        return(_put_value(n22, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4354(const uint8_t* s22)
{
    try
    {
        std::vector<int> n22 = _get_value<std::vector<int>>(s22, mlc_schema_table[5]);
        mlc::ArrowTable n23 = morloc_asCol(std::string("x"), n22);
        return(_put_value(n23, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4361(const uint8_t* s20)
{
    try
    {
        std::vector<int> n20 = _get_value<std::vector<int>>(s20, mlc_schema_table[14]);
        std::vector<int> n21 = {0,1,2};
        mlc::ArrowTable n22 = morloc_asCol(std::string("x"), n21);
        mlc::ArrowTable n23 = morloc_pickRows(n20, n22);
        return(_put_value(n23, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4367(const uint8_t* s23)
{
    try
    {
        std::vector<int> n23 = _get_value<std::vector<int>>(s23, mlc_schema_table[14]);
        mlc::ArrowTable n24 = morloc_asCol(std::string("x"), n23);
        return(_put_value(n24, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4474(const uint8_t* s24)
{
    try
    {
        std::vector<bool> n24 = _get_value<std::vector<bool>>(s24, mlc_schema_table[15]);
        std::vector<int> n25 = {0,1,2};
        mlc::ArrowTable n26 = morloc_asCol(std::string("x"), n25);
        mlc::ArrowTable n27 = morloc_filterRows(n24, n26);
        return(_put_value(n27, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4480()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4488(const uint8_t* s25)
{
    try
    {
        std::vector<bool> n25 = _get_value<std::vector<bool>>(s25, mlc_schema_table[15]);
        std::vector<int> n26 = {0,1,2};
        mlc::ArrowTable n27 = morloc_asCol(std::string("x"), n26);
        mlc::ArrowTable n28 = morloc_filterRows(n25, n27);
        int n29 = morloc_nrow(n28);
        return(_put_value(n29, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4502(const uint8_t* s26)
{
    try
    {
        std::vector<bool> n26 = _get_value<std::vector<bool>>(s26, mlc_schema_table[15]);
        std::vector<int> n27 = {0,1,2};
        mlc::ArrowTable n28 = morloc_asCol(std::string("x"), n27);
        mlc::ArrowTable n29 = morloc_filterRows(n26, n28);
        return(_put_value(n29, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4508(const uint8_t* s27)
{
    try
    {
        std::vector<int> n27 = _get_value<std::vector<int>>(s27, mlc_schema_table[3]);
        mlc::ArrowTable n28 = morloc_asCol(std::string("x"), n27);
        return(_put_value(n28, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4515(const uint8_t* s25)
{
    try
    {
        std::vector<bool> n25 = _get_value<std::vector<bool>>(s25, mlc_schema_table[15]);
        std::vector<int> n26 = {0,1,2};
        mlc::ArrowTable n27 = morloc_asCol(std::string("x"), n26);
        mlc::ArrowTable n28 = morloc_filterRows(n25, n27);
        std::vector<std::string> n29 = morloc_names(n28);
        return(_put_value(n29, mlc_schema_table[16]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4624()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4632(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(0, 0, n34);
        int n36 = morloc_nrow(n35);
        return(_put_value(n36, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4645(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(0, 0, n34);
        std::vector<std::string> n36 = morloc_names(n35);
        return(_put_value(n36, mlc_schema_table[16]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4659(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(0, 1, n34);
        return(_put_value(n35, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4664(const uint8_t* s28)
{
    try
    {
        std::vector<int> n28 = _get_value<std::vector<int>>(s28, mlc_schema_table[14]);
        mlc::ArrowTable n29 = morloc_asCol(std::string("x"), n28);
        return(_put_value(n29, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4673(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(0, 2, n34);
        return(_put_value(n35, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4678(const uint8_t* s31)
{
    try
    {
        std::vector<int> n31 = _get_value<std::vector<int>>(s31, mlc_schema_table[3]);
        mlc::ArrowTable n32 = morloc_asCol(std::string("x"), n31);
        return(_put_value(n32, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4687(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(0, 3, n34);
        return(_put_value(n35, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4692(const uint8_t* s33)
{
    try
    {
        std::vector<int> n33 = _get_value<std::vector<int>>(s33, mlc_schema_table[4]);
        mlc::ArrowTable n34 = morloc_asCol(std::string("x"), n33);
        return(_put_value(n34, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4701(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(1, 0, n34);
        int n36 = morloc_nrow(n35);
        return(_put_value(n36, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4714(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(1, 1, n34);
        int n36 = morloc_nrow(n35);
        return(_put_value(n36, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4727(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(1, 2, n34);
        return(_put_value(n35, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4732(const uint8_t* s29)
{
    try
    {
        std::vector<int> n29 = _get_value<std::vector<int>>(s29, mlc_schema_table[14]);
        mlc::ArrowTable n30 = morloc_asCol(std::string("x"), n29);
        return(_put_value(n30, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4741(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(1, 3, n34);
        return(_put_value(n35, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4746(const uint8_t* s32)
{
    try
    {
        std::vector<int> n32 = _get_value<std::vector<int>>(s32, mlc_schema_table[3]);
        mlc::ArrowTable n33 = morloc_asCol(std::string("x"), n32);
        return(_put_value(n33, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4755(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(1, 4, n34);
        return(_put_value(n35, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4760(const uint8_t* s32)
{
    try
    {
        std::vector<int> n32 = _get_value<std::vector<int>>(s32, mlc_schema_table[3]);
        mlc::ArrowTable n33 = morloc_asCol(std::string("x"), n32);
        return(_put_value(n33, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4769(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(2, 3, n34);
        return(_put_value(n35, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4774(const uint8_t* s30)
{
    try
    {
        std::vector<int> n30 = _get_value<std::vector<int>>(s30, mlc_schema_table[14]);
        mlc::ArrowTable n31 = morloc_asCol(std::string("x"), n30);
        return(_put_value(n31, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4783(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(3, 4, n34);
        int n36 = morloc_nrow(n35);
        return(_put_value(n36, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4794(const uint8_t* s34)
{
    try
    {
        mlc::ArrowTable n34 = _get_value<mlc::ArrowTable>(s34, mlc_schema_table[2]);
        mlc::ArrowTable n35 = morloc_sliceRows(5, 9, n34);
        int n36 = morloc_nrow(n35);
        return(_put_value(n36, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4907()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        std::vector<std::string> n3 = morloc_names(n2);
        return(_put_value(n3, mlc_schema_table[16]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4917()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<double> n1 = {0.0,0.5,1.0};
        mlc::ArrowTable n6 = morloc_setCol(std::string("z"), n1, n5);
        std::vector<std::string> n7 = morloc_names(n6);
        return(_put_value(n7, mlc_schema_table[16]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4981()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4985()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        return(_put_value(n2, mlc_schema_table[2]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m4993()
{
    try
    {
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n2 = morloc_asCol(std::string("y"), n1);
        return(_put_value(n2, mlc_schema_table[9]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m5000()
{
    try
    {
        std::vector<std::string> n1 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n2 = morloc_asCol(std::string("y"), n1);
        return(_put_value(n2, mlc_schema_table[9]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m5008()
{
    try
    {
        std::vector<int> n1 = {0,1,2};
        mlc::ArrowTable n2 = morloc_asCol(std::string("x"), n1);
        int n3 = morloc_nrow(n2);
        return(_put_value(n3, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
uint8_t* m5017()
{
    try
    {
        std::vector<int> n3 = {0,1,2};
        mlc::ArrowTable n4 = morloc_asCol(std::string("x"), n3);
        std::vector<std::string> n2 = {std::string("0")
        ,std::string("1")
        ,std::string("2")};
        mlc::ArrowTable n5 = morloc_setCol(std::string("y"), n2, n4);
        std::vector<double> n1 = {0.0,0.5,1.0};
        mlc::ArrowTable n6 = morloc_setCol(std::string("z"), n1, n5);
        int n7 = morloc_ncol(n6);
        return(_put_value(n7, mlc_schema_table[6]));
    } catch (...)
      {
          throw;
      }
}
// AUTO manifolds statements end



// AUTO dispatch start
uint8_t* local_dispatch(uint32_t mid, const uint8_t** args){
    switch(mid){
        case 1: return m1();
        case 2725: return m2725();
        case 2871: return m2871();
        case 2879: return m2879(args[0]);
        case 2884: return m2884(args[0]);
        case 2893: return m2893(args[0]);
        case 2905: return m2905(args[0]);
        case 2912: return m2912(args[0]);
        case 2926: return m2926(args[0]);
        case 2978: return m2978(args[0]);
        case 2987: return m2987();
        case 3003: return m3003();
        case 3017: return m3017();
        case 3031: return m3031();
        case 3039: return m3039();
        case 3173: return m3173();
        case 3180: return m3180();
        case 3187: return m3187();
        case 3254: return m3254();
        case 3259: return m3259();
        case 3265: return m3265();
        case 3361: return m3361();
        case 3368: return m3368();
        case 3376: return m3376();
        case 3383: return m3383();
        case 3389: return m3389();
        case 3395: return m3395();
        case 3527: return m3527();
        case 3532: return m3532();
        case 3540: return m3540();
        case 3546: return m3546();
        case 3552: return m3552();
        case 3559: return m3559();
        case 3690: return m3690(args[0]);
        case 3695: return m3695(args[0]);
        case 3707: return m3707();
        case 3714: return m3714();
        case 3722: return m3722();
        case 3729: return m3729();
        case 3887: return m3887();
        case 3899: return m3899();
        case 3907: return m3907();
        case 4022: return m4022();
        case 4031: return m4031();
        case 4039: return m4039(args[0]);
        case 4049: return m4049();
        case 4057: return m4057();
        case 4066: return m4066(args[0]);
        case 4073: return m4073();
        case 4079: return m4079();
        case 4178: return m4178(args[0]);
        case 4188: return m4188(args[0]);
        case 4196: return m4196(args[0]);
        case 4207: return m4207();
        case 4212: return m4212();
        case 4220: return m4220(args[0]);
        case 4223: return m4223(args[0]);
        case 4230: return m4230(args[0]);
        case 4319: return m4319(args[0]);
        case 4325: return m4325();
        case 4333: return m4333(args[0]);
        case 4339: return m4339(args[0]);
        case 4348: return m4348(args[0]);
        case 4354: return m4354(args[0]);
        case 4361: return m4361(args[0]);
        case 4367: return m4367(args[0]);
        case 4474: return m4474(args[0]);
        case 4480: return m4480();
        case 4488: return m4488(args[0]);
        case 4502: return m4502(args[0]);
        case 4508: return m4508(args[0]);
        case 4515: return m4515(args[0]);
        case 4624: return m4624();
        case 4632: return m4632(args[0]);
        case 4645: return m4645(args[0]);
        case 4659: return m4659(args[0]);
        case 4664: return m4664(args[0]);
        case 4673: return m4673(args[0]);
        case 4678: return m4678(args[0]);
        case 4687: return m4687(args[0]);
        case 4692: return m4692(args[0]);
        case 4701: return m4701(args[0]);
        case 4714: return m4714(args[0]);
        case 4727: return m4727(args[0]);
        case 4732: return m4732(args[0]);
        case 4741: return m4741(args[0]);
        case 4746: return m4746(args[0]);
        case 4755: return m4755(args[0]);
        case 4760: return m4760(args[0]);
        case 4769: return m4769(args[0]);
        case 4774: return m4774(args[0]);
        case 4783: return m4783(args[0]);
        case 4794: return m4794(args[0]);
        case 4907: return m4907();
        case 4917: return m4917();
        case 4981: return m4981();
        case 4985: return m4985();
        case 4993: return m4993();
        case 5000: return m5000();
        case 5008: return m5008();
        case 5017: return m5017();
        default:
            std::ostringstream oss;
            oss << "Invalid local manifold id: " << mid;
            throw std::runtime_error(oss.str());
    }
}

uint8_t* remote_dispatch(uint32_t mid, const uint8_t** args){
    switch(mid){
        
        default:
            std::ostringstream oss;
            oss << "Invalid remote manifold id: " << mid;
            throw std::runtime_error(oss.str());
    }
}
// AUTO dispatch end


// Wrappers to adapt compiler-generated dispatch functions to pool_dispatch_fn_t.
// These catch C++ exceptions so the C pool_main never sees them.
static uint8_t* cpp_local_dispatch(uint32_t mid, const uint8_t** args,
                                    size_t nargs, void* ctx) {
    (void)nargs; (void)ctx;
    // Free SHM from previous dispatch (result packet consumed by caller)
    _flush_shm_tracker();
    try {
        return local_dispatch(mid, args);
    } catch (const std::exception& e) {
        return make_fail_packet(e.what());
    } catch (...) {
        return make_fail_packet("An unknown error occurred");
    }
}

static uint8_t* cpp_remote_dispatch(uint32_t mid, const uint8_t** args,
                                     size_t nargs, void* ctx) {
    (void)nargs; (void)ctx;
    try {
        return remote_dispatch(mid, args);
    } catch (const std::exception& e) {
        return make_fail_packet(e.what());
    } catch (...) {
        return make_fail_packet("An unknown error occurred");
    }
}


int main(int argc, char* argv[]) {
    // Line-buffer stderr so diagnostic output is not lost on pool shutdown.
    // stdout is left fully buffered for performance (genome-scale piping)
    // and flushed after each job by pool.c.
    setvbuf(stderr, NULL, _IOLBF, 0);

    // Request SIGTERM when the parent (nexus) dies. Without this,
    // SIGKILL on the nexus leaves pool processes orphaned with
    // leaked SHM segments in /dev/shm.
#ifdef __linux__
    prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

    // Health check: confirm binary links and print version
    if (argc == 2 && std::string(argv[1]) == "--health") {
        std::cout << "{\"status\":\"ok\",\"version\":\"0.82.0\"}" << std::endl;
        return 0;
    }

    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <socket_path> <tmpdir> <shm_basename>\n";
        return 1;
    }

    g_tmpdir = strdup(argv[2]);

    pool_config_t config = {};
    config.local_dispatch = cpp_local_dispatch;
    config.remote_dispatch = cpp_remote_dispatch;
    config.dispatch_ctx = NULL;
    config.concurrency = POOL_THREADS;
    config.initial_workers = 1;
    config.dynamic_scaling = true;

    _init_schemas();
    int result = pool_main(argc, argv, &config);

    free(g_tmpdir);
    return result;
}
