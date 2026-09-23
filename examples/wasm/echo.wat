;; ArkFlow wasm processor example guest: echoes every row back unchanged.
;; Contract: exports `memory`, `alloc(len)->ptr`, `dealloc(ptr,len)` and
;; `transform(in_ptr, in_len) -> i64` packing the output as (ptr << 32) | len.
(module
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $next))
    (global.set $next (i32.add (local.get $ptr)
      (i32.and (i32.add (local.get $len) (i32.const 7)) (i32.const -8))))
    (local.get $ptr))
  (func (export "dealloc") (param $ptr i32) (param $len i32))
  (func (export "transform") (param $in_ptr i32) (param $in_len i32) (result i64)
    (local $ptr i32)
    (local.set $ptr (call 0 (local.get $in_len)))
    (memory.copy (local.get $ptr) (local.get $in_ptr) (local.get $in_len))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $in_len)))))
