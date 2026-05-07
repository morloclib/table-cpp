import signal
import sys
import select
import os # required for setting path to morloc dependencies
import time
import copy
import array
import struct
import socket as _socket
from collections import OrderedDict
from multiprocessing import Process, Value, RawValue
import ctypes
import functools


# Global variables for clean signal handling
daemon = None
workers = []
global_state = dict()
_shutdown_wakeup_fd = -1

# AUTO include sources start
sys.path = [os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")), os.path.expanduser("."), os.path.expanduser("/home/dev/.local/share/morloc/opt"), os.path.expanduser("/home/dev/.local/share/morloc/src/morloc/plane")] + sys.path
import importlib
import pymorloc as morloc
default_table_test_table_test = importlib.import_module("default.table.test.table-test")
mlc_schema_table = [ "<list>a:2<int>j"
, "<list>a:3<int>j"
, "T:11x<int>j"
, "<list>a:6<int>j"
, "<int>j"
, "T:21x<int>j1y<str>s"
, "T:11a<int>j"
, "T:11y<str>s"
, "<list>a:3<float>f8"
, "T:31x<int>j1y<str>s1z<float>f8"
, "<list>a:3<str>s"
, "<list>a:5<int>j"
, "<list>a:1<int>j"
, "<list>a:3<bool>b"
, "<list>a<str>s"
, "<tuple>t2<int>j<int>j" ]
# AUTO include sources end

# Dynamic worker spawning: monkey-patch foreign_call to track busy workers.
# Workers atomically increment busy_count before a foreign_call and decrement
# after. When busy_count reaches total_workers, a byte is written to a wake-up
# pipe to tell the main process to spawn a new worker.
_original_foreign_call = morloc.foreign_call
_busy_ref = None
_total_ref = None
_wakeup_fd = -1

def _init_worker_tracking(busy, total, wakeup_fd):
    global _busy_ref, _total_ref, _wakeup_fd
    _busy_ref = busy
    _total_ref = total
    _wakeup_fd = wakeup_fd
    morloc.foreign_call = _tracked_foreign_call

def _tracked_foreign_call(*args):
    prev = _busy_ref.value
    _busy_ref.value = prev + 1
    if prev + 1 >= _total_ref.value and _wakeup_fd >= 0:
        try:
            os.write(_wakeup_fd, b'!')
        except OSError:
            pass
    try:
        return _original_foreign_call(*args)
    finally:
        _busy_ref.value -= 1

# AUTO include manifolds start
def m2879(s38):
    try:
        s39 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2879
        , [s38] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2879):\n{e!s}")
    return(morloc.get_value(s39, mlc_schema_table[2]))

def m2884(s0):
    try:
        s40 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2884
        , [s0] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2884):\n{e!s}")
    return(morloc.get_value(s40, mlc_schema_table[2]))

def m2893(s38):
    try:
        s41 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2893
        , [s38] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2893):\n{e!s}")
    return(morloc.get_value(s41, mlc_schema_table[2]))

def m2905(s1):
    try:
        s42 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2905
        , [s1] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2905):\n{e!s}")
    return(morloc.get_value(s42, mlc_schema_table[2]))

def m2912(s38):
    try:
        s43 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2912
        , [s38] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2912):\n{e!s}")
    return(morloc.get_value(s43, mlc_schema_table[2]))

def m2926(s2):
    try:
        s44 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2926
        , [s2] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2926):\n{e!s}")
    return(morloc.get_value(s44, mlc_schema_table[2]))

def m2987():
    try:
        s47 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2987
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2987):\n{e!s}")
    return(morloc.get_value(s47, mlc_schema_table[4]))

def m3003():
    try:
        s48 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3003
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3003):\n{e!s}")
    return(morloc.get_value(s48, mlc_schema_table[2]))

def m3017():
    try:
        s49 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3017
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3017):\n{e!s}")
    return(morloc.get_value(s49, mlc_schema_table[4]))

def m3031():
    try:
        s50 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3031
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3031):\n{e!s}")
    return(morloc.get_value(s50, mlc_schema_table[5]))

def m3039():
    try:
        s51 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3039
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3039):\n{e!s}")
    return(morloc.get_value(s51, mlc_schema_table[5]))

def m3173():
    try:
        s52 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3173
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3173):\n{e!s}")
    return(morloc.get_value(s52, mlc_schema_table[6]))

def m3180():
    try:
        s53 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3180
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3180):\n{e!s}")
    return(morloc.get_value(s53, mlc_schema_table[6]))

def m3187():
    try:
        s54 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3187
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3187):\n{e!s}")
    return(morloc.get_value(s54, mlc_schema_table[1]))

def m3254():
    try:
        s56 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3254
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3254):\n{e!s}")
    return(morloc.get_value(s56, mlc_schema_table[2]))

def m3259():
    try:
        s57 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3259
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3259):\n{e!s}")
    return(morloc.get_value(s57, mlc_schema_table[2]))

def m3265():
    try:
        s58 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3265
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3265):\n{e!s}")
    return(morloc.get_value(s58, mlc_schema_table[4]))

def m3361():
    try:
        s59 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3361
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3361):\n{e!s}")
    return(morloc.get_value(s59, mlc_schema_table[2]))

def m3368():
    try:
        s60 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3368
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3368):\n{e!s}")
    return(morloc.get_value(s60, mlc_schema_table[2]))

def m3376():
    try:
        s61 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3376
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3376):\n{e!s}")
    return(morloc.get_value(s61, mlc_schema_table[7]))

def m3383():
    try:
        s62 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3383
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3383):\n{e!s}")
    return(morloc.get_value(s62, mlc_schema_table[7]))

def m3389():
    try:
        s63 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3389
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3389):\n{e!s}")
    return(morloc.get_value(s63, mlc_schema_table[5]))

def m3395():
    try:
        s64 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3395
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3395):\n{e!s}")
    return(morloc.get_value(s64, mlc_schema_table[5]))

def m3527():
    try:
        s65 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3527
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3527):\n{e!s}")
    return(morloc.get_value(s65, mlc_schema_table[5]))

def m3532():
    try:
        s66 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3532
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3532):\n{e!s}")
    return(morloc.get_value(s66, mlc_schema_table[5]))

def m3540():
    try:
        s67 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3540
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3540):\n{e!s}")
    return(morloc.get_value(s67, mlc_schema_table[7]))

def m3546():
    try:
        s68 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3546
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3546):\n{e!s}")
    return(morloc.get_value(s68, mlc_schema_table[7]))

def m3552():
    try:
        s69 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3552
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3552):\n{e!s}")
    return(morloc.get_value(s69, mlc_schema_table[2]))

def m3559():
    try:
        s70 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3559
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3559):\n{e!s}")
    return(morloc.get_value(s70, mlc_schema_table[2]))

def m3707():
    try:
        s74 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3707
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3707):\n{e!s}")
    return(morloc.get_value(s74, mlc_schema_table[5]))

def m3714():
    try:
        s75 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3714
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3714):\n{e!s}")
    return(morloc.get_value(s75, mlc_schema_table[5]))

def m3722():
    try:
        s76 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3722
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3722):\n{e!s}")
    return(morloc.get_value(s76, mlc_schema_table[9]))

def m3729():
    try:
        s77 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3729
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3729):\n{e!s}")
    return(morloc.get_value(s77, mlc_schema_table[9]))

def m3887():
    try:
        s78 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3887
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3887):\n{e!s}")
    return(morloc.get_value(s78, mlc_schema_table[1]))

def m3899():
    try:
        s80 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3899
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3899):\n{e!s}")
    return(morloc.get_value(s80, mlc_schema_table[10]))

def m3907():
    try:
        s82 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3907
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3907):\n{e!s}")
    return(morloc.get_value(s82, mlc_schema_table[8]))

def m4022():
    try:
        s85 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4022
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4022):\n{e!s}")
    return(morloc.get_value(s85, mlc_schema_table[2]))

def m4031():
    try:
        s86 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4031
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4031):\n{e!s}")
    return(morloc.get_value(s86, mlc_schema_table[2]))

def m4039(s9):
    try:
        s87 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4039
        , [s9] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4039):\n{e!s}")
    return(morloc.get_value(s87, mlc_schema_table[2]))

def m4049():
    try:
        s88 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4049
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4049):\n{e!s}")
    return(morloc.get_value(s88, mlc_schema_table[2]))

def m4057():
    try:
        s89 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4057
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4057):\n{e!s}")
    return(morloc.get_value(s89, mlc_schema_table[2]))

def m4066(s9):
    try:
        s90 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4066
        , [s9] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4066):\n{e!s}")
    return(morloc.get_value(s90, mlc_schema_table[2]))

def m4073():
    try:
        s91 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4073
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4073):\n{e!s}")
    return(morloc.get_value(s91, mlc_schema_table[2]))

def m4079():
    try:
        s92 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4079
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4079):\n{e!s}")
    return(morloc.get_value(s92, mlc_schema_table[2]))

def m4207():
    try:
        s100 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4207
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4207):\n{e!s}")
    return(morloc.get_value(s100, mlc_schema_table[2]))

def m4212():
    try:
        s101 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4212
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4212):\n{e!s}")
    return(morloc.get_value(s101, mlc_schema_table[2]))

def m4220(s94):
    try:
        s102 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4220
        , [s94] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4220):\n{e!s}")
    return(morloc.get_value(s102, mlc_schema_table[2]))

def m4223(s16):
    try:
        s103 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4223
        , [s16] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4223):\n{e!s}")
    return(morloc.get_value(s103, mlc_schema_table[2]))

def m4230(s96):
    try:
        s104 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4230
        , [s96] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4230):\n{e!s}")
    return(morloc.get_value(s104, mlc_schema_table[2]))

def m4319(s17):
    try:
        s112 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4319
        , [s17] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4319):\n{e!s}")
    return(morloc.get_value(s112, mlc_schema_table[2]))

def m4325():
    try:
        s113 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4325
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4325):\n{e!s}")
    return(morloc.get_value(s113, mlc_schema_table[2]))

def m4333(s18):
    try:
        s114 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4333
        , [s18] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4333):\n{e!s}")
    return(morloc.get_value(s114, mlc_schema_table[2]))

def m4339(s21):
    try:
        s115 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4339
        , [s21] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4339):\n{e!s}")
    return(morloc.get_value(s115, mlc_schema_table[2]))

def m4348(s19):
    try:
        s116 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4348
        , [s19] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4348):\n{e!s}")
    return(morloc.get_value(s116, mlc_schema_table[2]))

def m4354(s22):
    try:
        s117 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4354
        , [s22] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4354):\n{e!s}")
    return(morloc.get_value(s117, mlc_schema_table[2]))

def m4361(s20):
    try:
        s118 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4361
        , [s20] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4361):\n{e!s}")
    return(morloc.get_value(s118, mlc_schema_table[2]))

def m4367(s23):
    try:
        s119 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4367
        , [s23] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4367):\n{e!s}")
    return(morloc.get_value(s119, mlc_schema_table[2]))

def m4474(s24):
    try:
        s124 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4474
        , [s24] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4474):\n{e!s}")
    return(morloc.get_value(s124, mlc_schema_table[2]))

def m4480():
    try:
        s125 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4480
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4480):\n{e!s}")
    return(morloc.get_value(s125, mlc_schema_table[2]))

def m4488(s25):
    try:
        s126 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4488
        , [s25] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4488):\n{e!s}")
    return(morloc.get_value(s126, mlc_schema_table[4]))

def m4502(s26):
    try:
        s127 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4502
        , [s26] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4502):\n{e!s}")
    return(morloc.get_value(s127, mlc_schema_table[2]))

def m4508(s27):
    try:
        s128 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4508
        , [s27] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4508):\n{e!s}")
    return(morloc.get_value(s128, mlc_schema_table[2]))

def m4515(s25):
    try:
        s129 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4515
        , [s25] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4515):\n{e!s}")
    return(morloc.get_value(s129, mlc_schema_table[14]))

def m4632(s137):
    try:
        s138 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4632
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4632):\n{e!s}")
    return(morloc.get_value(s138, mlc_schema_table[4]))

def m4645(s137):
    try:
        s139 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4645
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4645):\n{e!s}")
    return(morloc.get_value(s139, mlc_schema_table[14]))

def m4659(s137):
    try:
        s141 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4659
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4659):\n{e!s}")
    return(morloc.get_value(s141, mlc_schema_table[2]))

def m4664(s28):
    try:
        s142 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4664
        , [s28] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4664):\n{e!s}")
    return(morloc.get_value(s142, mlc_schema_table[2]))

def m4673(s137):
    try:
        s143 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4673
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4673):\n{e!s}")
    return(morloc.get_value(s143, mlc_schema_table[2]))

def m4678(s31):
    try:
        s144 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4678
        , [s31] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4678):\n{e!s}")
    return(morloc.get_value(s144, mlc_schema_table[2]))

def m4687(s137):
    try:
        s145 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4687
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4687):\n{e!s}")
    return(morloc.get_value(s145, mlc_schema_table[2]))

def m4692(s33):
    try:
        s146 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4692
        , [s33] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4692):\n{e!s}")
    return(morloc.get_value(s146, mlc_schema_table[2]))

def m4701(s137):
    try:
        s147 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4701
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4701):\n{e!s}")
    return(morloc.get_value(s147, mlc_schema_table[4]))

def m4714(s137):
    try:
        s148 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4714
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4714):\n{e!s}")
    return(morloc.get_value(s148, mlc_schema_table[4]))

def m4727(s137):
    try:
        s149 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4727
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4727):\n{e!s}")
    return(morloc.get_value(s149, mlc_schema_table[2]))

def m4732(s29):
    try:
        s150 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4732
        , [s29] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4732):\n{e!s}")
    return(morloc.get_value(s150, mlc_schema_table[2]))

def m4741(s137):
    try:
        s151 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4741
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4741):\n{e!s}")
    return(morloc.get_value(s151, mlc_schema_table[2]))

def m4746(s32):
    try:
        s152 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4746
        , [s32] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4746):\n{e!s}")
    return(morloc.get_value(s152, mlc_schema_table[2]))

def m4755(s137):
    try:
        s153 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4755
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4755):\n{e!s}")
    return(morloc.get_value(s153, mlc_schema_table[2]))

def m4760(s32):
    try:
        s154 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4760
        , [s32] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4760):\n{e!s}")
    return(morloc.get_value(s154, mlc_schema_table[2]))

def m4769(s137):
    try:
        s155 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4769
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4769):\n{e!s}")
    return(morloc.get_value(s155, mlc_schema_table[2]))

def m4774(s30):
    try:
        s156 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4774
        , [s30] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4774):\n{e!s}")
    return(morloc.get_value(s156, mlc_schema_table[2]))

def m4783(s137):
    try:
        s157 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4783
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4783):\n{e!s}")
    return(morloc.get_value(s157, mlc_schema_table[4]))

def m4794(s137):
    try:
        s158 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4794
        , [s137] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4794):\n{e!s}")
    return(morloc.get_value(s158, mlc_schema_table[4]))

def m4907():
    try:
        s159 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4907
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4907):\n{e!s}")
    return(morloc.get_value(s159, mlc_schema_table[14]))

def m4917():
    try:
        s161 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4917
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4917):\n{e!s}")
    return(morloc.get_value(s161, mlc_schema_table[14]))

def m4981():
    try:
        s163 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4981
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4981):\n{e!s}")
    return(morloc.get_value(s163, mlc_schema_table[2]))

def m4985():
    try:
        s164 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4985
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4985):\n{e!s}")
    return(morloc.get_value(s164, mlc_schema_table[2]))

def m4993():
    try:
        s165 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4993
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4993):\n{e!s}")
    return(morloc.get_value(s165, mlc_schema_table[7]))

def m5000():
    try:
        s166 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 5000
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5000):\n{e!s}")
    return(morloc.get_value(s166, mlc_schema_table[7]))

def m5008():
    try:
        s167 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 5008
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5008):\n{e!s}")
    return(morloc.get_value(s167, mlc_schema_table[4]))

def m5017():
    try:
        s168 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 5017
        , [] )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5017):\n{e!s}")
    return(morloc.get_value(s168, mlc_schema_table[4]))

def m4973():
    try:
        s169 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2725
        , [] )
        n170 = default_table_test_table_test.printMsg( "asCol / nrow / ncol"
        , morloc.get_value(s169, mlc_schema_table[15]) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4973):\n{e!s}")
    return(n170)

def m5058():
    try:
        n171 = default_table_test_table_test.testEqual( "ncol (mkXYZ3) = 3"
        , m5017()
        , 3
        , m4973() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5058):\n{e!s}")
    return(n171)

def m5043():
    try:
        n172 = default_table_test_table_test.testEqual( "nrow (mkX 3) = 3"
        , m5008()
        , 3
        , m5058() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5043):\n{e!s}")
    return(n172)

def m5024():
    try:
        n173 = default_table_test_table_test.testEqual( "asCol from List literal"
        , m4993()
        , m5000()
        , m5043() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m5024):\n{e!s}")
    return(n173)

def m4886():
    try:
        n174 = default_table_test_table_test.testEqual( "asCol \"x\" xs3 == mkX 3"
        , m4981()
        , m4985()
        , m5024() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4886):\n{e!s}")
    return(n174)

def m4899():
    try:
        n175 = default_table_test_table_test.printMsg("names", m4886())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4899):\n{e!s}")
    return(n175)

def m4927():
    try:
        n162 = ["x", "y", "z"]
        n176 = default_table_test_table_test.testEqual( "names mkXYZ3 = [\"x\", \"y\", \"z\"]"
        , m4917()
        , n162
        , m4899() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4927):\n{e!s}")
    return(n176)

def m4591():
    try:
        n160 = ["x"]
        n177 = default_table_test_table_test.testEqual( "names (mkX 3) = [\"x\"]"
        , m4907()
        , n160
        , m4927() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4591):\n{e!s}")
    return(n177)

def m4604():
    try:
        n178 = default_table_test_table_test.printMsg("sliceRows", m4591())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4604):\n{e!s}")
    return(n178)

def m4880(s137):
    try:
        n179 = default_table_test_table_test.testEqual( "sliceRows 5 9 t == empty (start past end)"
        , m4794(s137)
        , 0
        , m4604() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4880):\n{e!s}")
    return(n179)

def m4874(s137):
    try:
        n180 = default_table_test_table_test.testEqual( "sliceRows 3 4 t == empty (start at end)"
        , m4783(s137)
        , 0
        , m4880(s137) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4874):\n{e!s}")
    return(n180)

def m4868(s137, s30):
    try:
        n181 = default_table_test_table_test.testEqual( "sliceRows 2 3 t == [2] (last row)"
        , m4769(s137)
        , m4774(s30)
        , m4874(s137) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4868):\n{e!s}")
    return(n181)

def m4862(s137, s32, s30):
    try:
        n182 = default_table_test_table_test.testEqual( "sliceRows 1 4 t == [1, 2] (end past nrow, clamped)"
        , m4755(s137)
        , m4760(s32)
        , m4868(s137, s30) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4862):\n{e!s}")
    return(n182)

def m4856(s137, s32, s30):
    try:
        n183 = default_table_test_table_test.testEqual( "sliceRows 1 3 t == [1, 2]"
        , m4741(s137)
        , m4746(s32)
        , m4862(s137, s32, s30) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4856):\n{e!s}")
    return(n183)

def m4850(s137, s32, s30, s29):
    try:
        n184 = default_table_test_table_test.testEqual( "sliceRows 1 2 t == [1] (single row)"
        , m4727(s137)
        , m4732(s29)
        , m4856(s137, s32, s30) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4850):\n{e!s}")
    return(n184)

def m4844(s137, s32, s30, s29):
    try:
        n185 = default_table_test_table_test.testEqual( "sliceRows 1 1 t == empty (end == start)"
        , m4714(s137)
        , 0
        , m4850(s137, s32, s30, s29) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4844):\n{e!s}")
    return(n185)

def m4838(s137, s32, s30, s29):
    try:
        n186 = default_table_test_table_test.testEqual( "sliceRows 1 0 t == empty (end < start, no reverse)"
        , m4701(s137)
        , 0
        , m4844(s137, s32, s30, s29) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4838):\n{e!s}")
    return(n186)

def m4832(s137, s33, s32, s30, s29):
    try:
        n187 = default_table_test_table_test.testEqual( "sliceRows 0 3 t == full table"
        , m4687(s137)
        , m4692(s33)
        , m4838(s137, s32, s30, s29) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4832):\n{e!s}")
    return(n187)

def m4826(s137, s33, s32, s31, s30, s29):
    try:
        n188 = default_table_test_table_test.testEqual( "sliceRows 0 2 t == [0, 1]"
        , m4673(s137)
        , m4678(s31)
        , m4832(s137, s33, s32, s30, s29) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4826):\n{e!s}")
    return(n188)

def m4820(s137, s33, s32, s31, s30, s29, s28):
    try:
        n189 = default_table_test_table_test.testEqual( "sliceRows 0 1 t == [0]"
        , m4659(s137)
        , m4664(s28)
        , m4826(s137, s33, s32, s31, s30, s29) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4820):\n{e!s}")
    return(n189)

def m4814(s137, s33, s32, s31, s30, s29, s28):
    try:
        n140 = ["x"]
        n190 = default_table_test_table_test.testEqual( "sliceRows 0 0 t preserves schema"
        , m4645(s137)
        , n140
        , m4820(s137, s33, s32, s31, s30, s29, s28) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4814):\n{e!s}")
    return(n190)

def m4437(s137, s33, s32, s31, s30, s29, s28):
    try:
        n191 = default_table_test_table_test.testEqual( "sliceRows 0 0 t == empty"
        , m4632(s137)
        , 0
        , m4814(s137, s33, s32, s31, s30, s29, s28) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4437):\n{e!s}")
    return(n191)

def m4450():
    try:
        n131 = [0]
        s28 = morloc.put_value(n131, mlc_schema_table[12])
        n132 = [1]
        s29 = morloc.put_value(n132, mlc_schema_table[12])
        n133 = [2]
        s30 = morloc.put_value(n133, mlc_schema_table[12])
        n134 = [0, 1]
        s31 = morloc.put_value(n134, mlc_schema_table[0])
        n135 = [1, 2]
        s32 = morloc.put_value(n135, mlc_schema_table[0])
        n136 = [0, 1, 2]
        s33 = morloc.put_value(n136, mlc_schema_table[1])
        s137 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4624
        , [] )
        n192 = m4437(s137, s33, s32, s31, s30, s29, s28)
        n193 = default_table_test_table_test.printMsg("filterRows", n192)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4450):\n{e!s}")
    return(n193)

def m4567(s25):
    try:
        n130 = ["x"]
        n194 = default_table_test_table_test.testEqual( "filterRows allFalse preserves schema"
        , m4515(s25)
        , n130
        , m4450() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4567):\n{e!s}")
    return(n194)

def m4552(s27, s26, s25):
    try:
        n195 = default_table_test_table_test.testEqual( "filterRows mixed (mkX 3) keeps positions 0, 2"
        , m4502(s26)
        , m4508(s27)
        , m4567(s25) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4552):\n{e!s}")
    return(n195)

def m4528(s27, s26, s25):
    try:
        n196 = default_table_test_table_test.testEqual( "filterRows allFalse (mkX 3) nrow = 0"
        , m4488(s25)
        , 0
        , m4552(s27, s26, s25) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4528):\n{e!s}")
    return(n196)

def m4267(s27, s26, s25, s24):
    try:
        n197 = default_table_test_table_test.testEqual( "filterRows allTrue (mkX 3) == mkX 3"
        , m4474(s24)
        , m4480()
        , m4528(s27, s26, s25) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4267):\n{e!s}")
    return(n197)

def m4280():
    try:
        n120 = [True, True, True]
        s24 = morloc.put_value(n120, mlc_schema_table[13])
        n121 = [False, False, False]
        s25 = morloc.put_value(n121, mlc_schema_table[13])
        n122 = [True, False, True]
        s26 = morloc.put_value(n122, mlc_schema_table[13])
        n123 = [0, 2]
        s27 = morloc.put_value(n123, mlc_schema_table[0])
        n198 = m4267(s27, s26, s25, s24)
        n199 = default_table_test_table_test.printMsg("pickRows", n198)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4280):\n{e!s}")
    return(n199)

def m4413(s23, s20):
    try:
        n200 = default_table_test_table_test.testEqual( "pickRows [1] (mkX 3) picks the middle row"
        , m4361(s20)
        , m4367(s23)
        , m4280() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4413):\n{e!s}")
    return(n200)

def m4398(s23, s22, s20, s19):
    try:
        n201 = default_table_test_table_test.testEqual( "pickRows duped (mkX 3) duplicates rows"
        , m4348(s19)
        , m4354(s22)
        , m4413(s23, s20) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4398):\n{e!s}")
    return(n201)

def m4374(s23, s22, s21, s20, s19, s18):
    try:
        n202 = default_table_test_table_test.testEqual( "pickRows reversed (mkX 3) reverses"
        , m4333(s18)
        , m4339(s21)
        , m4398(s23, s22, s20, s19) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4374):\n{e!s}")
    return(n202)

def m4157(s23, s22, s21, s20, s19, s18, s17):
    try:
        n203 = default_table_test_table_test.testEqual( "pickRows identity (mkX 3) == mkX 3"
        , m4319(s17)
        , m4325()
        , m4374(s23, s22, s21, s20, s19, s18) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4157):\n{e!s}")
    return(n203)

def m4170():
    try:
        n105 = [0, 1, 2]
        s17 = morloc.put_value(n105, mlc_schema_table[1])
        n106 = [2, 1, 0]
        s18 = morloc.put_value(n106, mlc_schema_table[1])
        n107 = [0, 0, 1, 1, 2, 2]
        s19 = morloc.put_value(n107, mlc_schema_table[3])
        n108 = [1]
        s20 = morloc.put_value(n108, mlc_schema_table[12])
        n109 = [2, 1, 0]
        s21 = morloc.put_value(n109, mlc_schema_table[1])
        n110 = [0, 0, 1, 1, 2, 2]
        s22 = morloc.put_value(n110, mlc_schema_table[3])
        n111 = [1]
        s23 = morloc.put_value(n111, mlc_schema_table[12])
        n204 = m4157(s23, s22, s21, s20, s19, s18, s17)
        n205 = default_table_test_table_test.printMsg("distinctRows", n204)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4170):\n{e!s}")
    return(n205)

def m4261(n15, s96):
    try:
        n206 = default_table_test_table_test.testEqual( "distinctRows of [1,2,1,3,2] == [1,2,3]"
        , m4230(s96)
        , n15
        , m4170() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4261):\n{e!s}")
    return(n206)

def m4237(s16, n15, s96, s94):
    try:
        n207 = default_table_test_table_test.testEqual( "distinctRows of all-dup [7,7,7] == [7]"
        , m4220(s94)
        , m4223(s16)
        , m4261(n15, s96) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4237):\n{e!s}")
    return(n207)

def m3996(s16, n15, s96, s94):
    try:
        n208 = default_table_test_table_test.testEqual( "distinctRows of all-distinct (mkX 3) == mkX 3"
        , m4207()
        , m4212()
        , m4237(s16, n15, s96, s94) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3996):\n{e!s}")
    return(n208)

def m4009():
    try:
        n93 = [7, 7, 7]
        s10 = morloc.put_value(n93, mlc_schema_table[1])
        s94 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4178
        , [s10] )
        n95 = [1, 2, 1, 3, 2]
        s12 = morloc.put_value(n95, mlc_schema_table[11])
        s96 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4188
        , [s12] )
        n97 = [1, 2, 3]
        s14 = morloc.put_value(n97, mlc_schema_table[1])
        s98 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 4196
        , [s14] )
        n15 = morloc.get_value(s98, mlc_schema_table[2])
        n99 = [7]
        s16 = morloc.put_value(n99, mlc_schema_table[12])
        n209 = m3996(s16, n15, s96, s94)
        n210 = default_table_test_table_test.printMsg("sortRows", n209)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4009):\n{e!s}")
    return(n210)

def m4124():
    try:
        n211 = default_table_test_table_test.testEqual( "sortRows [] (mkX 3) is a no-op"
        , m4073()
        , m4079()
        , m4009() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4124):\n{e!s}")
    return(n211)

def m4109(s9):
    try:
        n212 = default_table_test_table_test.testEqual( "sortRows [(\"x\", False)] (mkX 3) reverses"
        , m4057()
        , m4066(s9)
        , m4124() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4109):\n{e!s}")
    return(n212)

def m4085(s9):
    try:
        n213 = default_table_test_table_test.testEqual( "sortRows [(\"x\", True)] of [2, 1, 0] == mkX 3"
        , m4039(s9)
        , m4049()
        , m4109(s9) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4085):\n{e!s}")
    return(n213)

def m3866(s9):
    try:
        n214 = default_table_test_table_test.testEqual( "sortRows [(\"x\", True)] (mkX 3) == mkX 3 (already asc)"
        , m4022()
        , m4031()
        , m4085(s9) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3866):\n{e!s}")
    return(n214)

def m3879():
    try:
        n84 = [2, 1, 0]
        s9 = morloc.put_value(n84, mlc_schema_table[1])
        n215 = m3866(s9)
        n216 = default_table_test_table_test.printMsg("getCol", n215)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3879):\n{e!s}")
    return(n216)

def m3934():
    try:
        n83 = [0.0, 0.5, 1.0]
        n217 = default_table_test_table_test.testEqual( "getCol \"z\" mkXYZ3 == zs3"
        , m3907()
        , n83
        , m3879() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3934):\n{e!s}")
    return(n217)

def m3915():
    try:
        n81 = ["0", "1", "2"]
        n218 = default_table_test_table_test.testEqual( "getCol \"y\" mkXYZ3 == ys3"
        , m3899()
        , n81
        , m3934() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3915):\n{e!s}")
    return(n218)

def m3669():
    try:
        n79 = [0, 1, 2]
        n219 = default_table_test_table_test.testEqual( "getCol \"x\" (mkX 3) == xs3"
        , m3887()
        , n79
        , m3915() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3669):\n{e!s}")
    return(n219)

def m3682():
    try:
        n220 = default_table_test_table_test.printMsg("setCol", m3669())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3682):\n{e!s}")
    return(n220)

def m3815(n8, n7):
    try:
        n221 = default_table_test_table_test.testEqual( "setCol \"z\" newZs mkXYZ3 replaces the existing z column"
        , n7
        , n8
        , m3682() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3815):\n{e!s}")
    return(n221)

def m3779(n8, n7):
    try:
        n222 = default_table_test_table_test.testEqual( "setCol \"z\" zs (mkXY 3) == mkXYZ3 (append new column)"
        , m3722()
        , m3729()
        , m3815(n8, n7) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3779):\n{e!s}")
    return(n222)

def m3506(n8, n7):
    try:
        n223 = default_table_test_table_test.testEqual( "setCol \"y\" ys (mkX 3) == mkXY 3 (append new column)"
        , m3707()
        , m3714()
        , m3779(n8, n7) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3506):\n{e!s}")
    return(n223)

def m3519():
    try:
        n71 = [9.0, 9.0, 9.0]
        s6 = morloc.put_value(n71, mlc_schema_table[8])
        s72 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3690
        , [s6] )
        n7 = morloc.get_value(s72, mlc_schema_table[9])
        s73 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 3695
        , [s6] )
        n8 = morloc.get_value(s73, mlc_schema_table[9])
        n224 = m3506(n8, n7)
        n225 = default_table_test_table_test.printMsg("dropCols", n224)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3519):\n{e!s}")
    return(n225)

def m3612():
    try:
        n226 = default_table_test_table_test.testEqual( "dropCols [\"absent\"] (mkX 3) == mkX 3 (no-op)"
        , m3552()
        , m3559()
        , m3519() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3612):\n{e!s}")
    return(n226)

def m3565():
    try:
        n227 = default_table_test_table_test.testEqual( "dropCols [\"x\", \"z\"] mkXYZ3 == mkY 3"
        , m3540()
        , m3546()
        , m3612() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3565):\n{e!s}")
    return(n227)

def m3340():
    try:
        n228 = default_table_test_table_test.testEqual( "dropCols [\"z\"] mkXYZ3 == mkXY 3"
        , m3527()
        , m3532()
        , m3565() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3340):\n{e!s}")
    return(n228)

def m3353():
    try:
        n229 = default_table_test_table_test.printMsg("selectCols", m3340())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3353):\n{e!s}")
    return(n229)

def m3433():
    try:
        n230 = default_table_test_table_test.testEqual( "selectCols [\"x\", \"y\"] mkXYZ3 == mkXY 3"
        , m3389()
        , m3395()
        , m3353() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3433):\n{e!s}")
    return(n230)

def m3401():
    try:
        n231 = default_table_test_table_test.testEqual( "selectCols [\"y\"] (mkXY 3) == mkY 3"
        , m3376()
        , m3383()
        , m3433() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3401):\n{e!s}")
    return(n231)

def m3233():
    try:
        n232 = default_table_test_table_test.testEqual( "selectCols [\"x\"] (mkXY 3) == mkX 3"
        , m3361()
        , m3368()
        , m3401() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3233):\n{e!s}")
    return(n232)

def m3246():
    try:
        n233 = default_table_test_table_test.printMsg("selectColsDyn", m3233())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3246):\n{e!s}")
    return(n233)

def m3277():
    try:
        n234 = default_table_test_table_test.testEqual( "selectColsDyn [\"y\", \"z\"] mkXYZ3 keeps two columns"
        , m3265()
        , 2
        , m3246() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3277):\n{e!s}")
    return(n234)

def m3152():
    try:
        n235 = default_table_test_table_test.testEqual( "selectColsDyn [\"x\"] mkXYZ3 == mkX 3"
        , m3254()
        , m3259()
        , m3277() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3152):\n{e!s}")
    return(n235)

def m3165():
    try:
        n236 = default_table_test_table_test.printMsg("renameCol", m3152())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3165):\n{e!s}")
    return(n236)

def m3201():
    try:
        n55 = [0, 1, 2]
        n237 = default_table_test_table_test.testEqual( "rename then getCol round-trip"
        , m3187()
        , n55
        , m3165() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3201):\n{e!s}")
    return(n237)

def m2954():
    try:
        n238 = default_table_test_table_test.testEqual( "renameCol \"x\" \"a\" (mkX 3) renames"
        , m3173()
        , m3180()
        , m3201() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2954):\n{e!s}")
    return(n238)

def m2967():
    try:
        n239 = default_table_test_table_test.printMsg("rbind / cbind", m2954())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2967):\n{e!s}")
    return(n239)

def m3093():
    try:
        n240 = default_table_test_table_test.testEqual( "cbind (mkX 3) (mkY 3) == mkXY 3"
        , m3031()
        , m3039()
        , m2967() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3093):\n{e!s}")
    return(n240)

def m3069():
    try:
        n241 = default_table_test_table_test.testEqual( "cbind (mkX 3) (mkY 3) ncol = 2"
        , m3017()
        , 2
        , m3093() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3069):\n{e!s}")
    return(n241)

def m3045(n5):
    try:
        n242 = default_table_test_table_test.testEqual( "rbind (mkX 3) (mkX 3) == doubled column"
        , m3003()
        , n5
        , m3069() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3045):\n{e!s}")
    return(n242)

def m2844(n5):
    try:
        n243 = default_table_test_table_test.testEqual( "rbind (mkX 3) (mkX 3) nrow = 6"
        , m2987()
        , 6
        , m3045(n5) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2844):\n{e!s}")
    return(n243)

def m2857():
    try:
        n45 = [0, 1, 2, 0, 1, 2]
        s4 = morloc.put_value(n45, mlc_schema_table[3])
        s46 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2978
        , [s4] )
        n5 = morloc.get_value(s46, mlc_schema_table[2])
        n244 = m2844(n5)
        n245 = default_table_test_table_test.printMsg( "Composition / derived"
        , n244 )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2857):\n{e!s}")
    return(n245)

def m2948(s38, s2):
    try:
        n246 = default_table_test_table_test.testEqual( "reverseRows t == pickRows (reverse (range 0 (nrow t - 1))) t"
        , m2912(s38)
        , m2926(s2)
        , m2857() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2948):\n{e!s}")
    return(n246)

def m2942(s38, s2, s1):
    try:
        n247 = default_table_test_table_test.testEqual( "tail k t == sliceRows (nrow t - k) (nrow t) t"
        , m2893(s38)
        , m2905(s1)
        , m2948(s38, s2) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2942):\n{e!s}")
    return(n247)

def m2838(s38, s2, s1, s0):
    try:
        n248 = default_table_test_table_test.testEqual( "head k t == sliceRows 0 k t"
        , m2879(s38)
        , m2884(s0)
        , m2942(s38, s2, s1) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2838):\n{e!s}")
    return(n248)

def m2825():
    try:
        n35 = [0, 1]
        s0 = morloc.put_value(n35, mlc_schema_table[0])
        n36 = [1, 2]
        s1 = morloc.put_value(n36, mlc_schema_table[0])
        n37 = [2, 1, 0]
        s2 = morloc.put_value(n37, mlc_schema_table[1])
        s38 = morloc.foreign_call( os.path.join(global_state["tmpdir"], "pipe-cpp")
        , 2871
        , [] )
        n249 = m2838(s38, s2, s1, s0)
        n250 = default_table_test_table_test.printResult(n249)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2825):\n{e!s}")
    return(morloc.put_value(n250, mlc_schema_table[15]))
# AUTO include manifolds end


# AUTO include dispatch start
dispatch = {
    2825: m2825,
}
remote_dispatch = {
}
# AUTO include dispatch end

def run_job(client_fd: int) -> None:
    try:
        # Free SHM from previous dispatch result (consumed by caller)
        morloc.flush_shm_tracker()
        client_data = morloc.stream_from_client(client_fd)

        if(morloc.is_local_call(client_data)):
            (mid, args) = morloc.read_morloc_call_packet(client_data)

            try:
                result = dispatch[mid](*args)
            except Exception as e:
                result = morloc.make_fail_packet(str(e))

        elif(morloc.is_remote_call(client_data)):
            (mid, args) = morloc.read_morloc_call_packet(client_data)

            try:
                result = remote_dispatch[mid](*args)
            except Exception as e:
                result = morloc.make_fail_packet(str(e))

        elif(morloc.is_ping(client_data)):
            result = morloc.pong(client_data)

        else:
            raise ValueError("Expected a ping or call type packet")

        # Flush stdout BEFORE sending the result back. The nexus prints its
        # own output (the return value) right after receiving this response.
        # Both processes share the same stdout fd, so if we flush after sending,
        # the nexus can print first, causing out-of-order output.
        sys.stdout.flush()

        morloc.send_packet_to_foreign_server(client_fd, result)

    except Exception as e:
        # Try to send a fail packet back to the caller before giving up.
        # This may fail (e.g., broken pipe from a timed-out ping), which is OK.
        try:
            result = morloc.make_fail_packet(str(e))
            morloc.send_packet_to_foreign_server(client_fd, result)
        except Exception:
            pass
        print(f"job failed: {e!s}", file=sys.stderr)
    finally:
        # Safety-net flush for any output from error handling paths
        sys.stdout.flush()
        # close child copy
        morloc.close_socket(client_fd)


def _send_fd(sock, fd):
    """Send a file descriptor over a Unix domain socket."""
    sock.sendmsg([b'\x00'],
                 [(_socket.SOL_SOCKET, _socket.SCM_RIGHTS,
                   array.array('i', [fd]))])

def _recv_fd(sock):
    """Receive a file descriptor from a Unix domain socket."""
    msg, ancdata, flags, addr = sock.recvmsg(1, _socket.CMSG_SPACE(4))
    if not msg and not ancdata:
        raise EOFError("Connection closed")
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if (cmsg_level == _socket.SOL_SOCKET and
                cmsg_type == _socket.SCM_RIGHTS):
            a = array.array('i')
            a.frombytes(cmsg_data[:4])
            return a[0]
    raise RuntimeError("No fd received in ancillary data")


WORKER_IDLE_TIMEOUT = 5.0  # seconds before an idle worker exits

def worker_process(job_fd, tmpdir, shm_basename, shutdown_flag, busy_count, total_workers, wakeup_w):
    # Reset signal handlers inherited from main. If user code inside run_job
    # calls multiprocessing.Pool (or anything else that forks and later
    # SIGTERMs its own children), those grandchildren would otherwise inherit
    # main's signal_handler and flip the shared shutdown_flag, causing main
    # to SIGKILL this worker mid-response. See the multiprocessing-py-1 bug.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    morloc.set_fallback_dir(tmpdir)
    morloc.shinit(shm_basename, 0, 0xffff)
    _init_worker_tracking(busy_count, total_workers, wakeup_w)
    sock = _socket.fromfd(job_fd, _socket.AF_UNIX, _socket.SOCK_STREAM)
    os.close(job_fd)  # sock owns a dup'd copy
    last_activity = time.monotonic()
    try:
        while not shutdown_flag.value:
            rlist, _, _ = select.select([sock.fileno()], [], [], 0.01)
            if shutdown_flag.value:
                break
            if rlist:
                try:
                    client_fd = _recv_fd(sock)
                    run_job(client_fd)
                    last_activity = time.monotonic()
                except (EOFError, OSError):
                    break
            elif total_workers.value > 1 and time.monotonic() - last_activity > WORKER_IDLE_TIMEOUT:
                break
    except BaseException as e:
        # Catch-all for errors that escape run_job's own exception handling:
        # MemoryError, KeyboardInterrupt, SystemExit, or bugs in the worker
        # loop itself. Without this, the worker dies silently and the nexus
        # only sees "failed to read response header" with no indication of
        # what went wrong in the pool.
        #
        # Race condition: the nexus detects the broken socket and may start
        # its clean_exit tear-down (SIGTERM -> SIGKILL) while this print is
        # still buffered. We flush immediately to maximize the chance the
        # message reaches the terminal before we are killed. stderr is
        # line-buffered (set in __main__), but the flush is a safety net for
        # edge cases (redirected stderr, forked-process buffer state).
        import traceback
        print(f"morloc pool worker fatal error: {e!s}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
    finally:
        sock.close()


def signal_handler(sig, frame):
    global daemon
    # Ignore further SIGTERM/SIGINT during cleanup. Python processes pending
    # signals between bytecodes, including while another signal handler is
    # running, so a second SIGTERM arriving mid-cleanup would otherwise
    # re-enter this handler and double-free the daemon pointer.
    try:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception:
        pass
    shutdown_flag.value = True
    if _shutdown_wakeup_fd >= 0:
        try:
            os.write(_shutdown_wakeup_fd, b'!')
        except OSError:
            pass
    # Capture the daemon pointer into a local and clear the global BEFORE
    # invoking close_daemon. If a pending signal still slips through and
    # re-enters this handler, it will see daemon=None and skip the free.
    d = daemon
    daemon = None
    if d is not None:
        morloc.close_daemon(d)


def client_listener(job_fd, socket_path, tmpdir, shm_basename, shutdown_flag):
    global daemon
    daemon = morloc.start_daemon(socket_path, tmpdir, shm_basename, 0xffff)
    sock = _socket.fromfd(job_fd, _socket.AF_UNIX, _socket.SOCK_STREAM)
    os.close(job_fd)  # sock owns a dup'd copy

    while not shutdown_flag.value:
        try:
            client_fd = morloc.wait_for_client(daemon)
        except Exception as e:
            print(f"In python daemon, failed to connect to client: {e!s}", file=sys.stderr)
            continue

        if client_fd > 0:
            try:
                _send_fd(sock, client_fd)
            except Exception as e:
                print(f"In python daemon, failed to start worker: {e!s}", file=sys.stderr)
            finally:
                morloc.close_socket(client_fd)
    sock.close()



if __name__ == "__main__":
    # Line-buffer stderr so diagnostic output is not lost when pool is killed.
    # stdout is left fully buffered for performance (genome-scale piping) and
    # flushed explicitly after each job and during shutdown.
    sys.stderr.reconfigure(line_buffering=True)

    # Request SIGTERM when the parent process (nexus) dies.
    # Without this, SIGKILL on the nexus leaves pool processes orphaned
    # and their SHM segments leak in /dev/shm.
    try:
        import ctypes
        _PR_SET_PDEATHSIG = 1
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
    except Exception:
        pass  # non-Linux: skip (macOS uses kqueue for this)

    shutdown_flag = Value('b', False)  # Shared flag

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Health check: confirm imports loaded and print version
    if len(sys.argv) > 1 and sys.argv[1] == "--health":
        sys.stdout.write('{"status":"ok","version":"0.82.0"}\n')
        sys.exit(0)

    # Process arguments passed from the nexus
    try:
        socket_path = sys.argv[1]
        tmpdir = sys.argv[2]
        shm_basename = sys.argv[3]
    except IndexError:
        print("Usage: script.py <socket_path> <tmpdir> <shm_basename>")
        sys.exit(1)

    global_state["tmpdir"] = tmpdir

    # Shared job queue: listener writes fds to write_sock, workers read from read_sock.
    # Only idle workers (blocked in recvmsg) pick up jobs, preventing the round-robin
    # deadlock where a callback gets dispatched to a busy worker.
    read_sock, write_sock = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)

    num_workers = 1
    workers = []

    # Shared counters for dynamic worker spawning.
    # Workers increment busy_count before foreign_call and decrement after.
    # When all workers are busy, main process spawns a new one.
    busy_count = RawValue(ctypes.c_int, 0)
    total_workers = RawValue(ctypes.c_int, num_workers)
    wakeup_r, wakeup_w = os.pipe()
    os.set_blocking(wakeup_r, False)
    _shutdown_wakeup_fd = wakeup_w

    # Keep a dup of the read end so we can spawn new workers later
    spare_read_fd = os.dup(read_sock.fileno())

    for i in range(num_workers):
        worker = Process(target=worker_process,
                         args=(read_sock.fileno(), tmpdir, shm_basename, shutdown_flag,
                               busy_count, total_workers, wakeup_w))
        worker.start()
        workers.append(worker)
    read_sock.close()  # main/listener don't need the read end (spare_read_fd kept)

    # Start client listener process
    listener_process = Process(
        target=client_listener,
        args=(write_sock.fileno(), socket_path, tmpdir, shm_basename, shutdown_flag)
    )
    listener_process.start()
    write_sock.close()  # main doesn't need the write end

    # Main loop: monitor wake-up pipe, spawn new workers when all are busy,
    # and reap idle workers that have exited.
    while not shutdown_flag.value:
        rlist, _, _ = select.select([wakeup_r], [], [], 0.01)
        if rlist:
            try:
                os.read(wakeup_r, 4096)  # drain pipe
            except OSError:
                pass

        # Reap dead workers (idle timeout or error exit)
        alive = []
        for w in workers:
            if w.is_alive():
                alive.append(w)
            else:
                w.join(timeout=0)
                w.close()
        workers = alive
        total_workers.value = max(1, len(workers))

        # Spawn a new worker if all are busy (or all have exited)
        if len(workers) == 0 or busy_count.value >= total_workers.value:
            w = Process(target=worker_process,
                        args=(spare_read_fd, tmpdir, shm_basename, shutdown_flag,
                              busy_count, total_workers, wakeup_w))
            w.start()
            workers.append(w)
            total_workers.value = len(workers)

    # Shutdown sequence
    os.close(wakeup_r)
    os.close(wakeup_w)
    os.close(spare_read_fd)

    # 1. Stop listener first
    listener_process.terminate()
    listener_process.join(timeout=0.001)
    listener_process.kill()
    listener_process.join()  # Final blocking reap
    listener_process.close()

    # 2. Terminate workers with escalating force
    for p in workers:
        if p.is_alive():
            p.kill()
        p.join()  # Final blocking reap
        p.close()

    sys.exit(0)
