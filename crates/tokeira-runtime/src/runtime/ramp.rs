//! Deterministic workflow-ID selection for Worker Deployment ramps.
//!
//! The sending node, receiving node, and task-start path must select the same
//! target from the same routing config; this threshold is computed, never stored.
//! Selection follows `common/worker_versioning/worker_versioning.go @ v1.31.0`.
//! Fingerprint32 follows `farmhashmk.go`, `farmhashcc.go`, and `basics.go` in
//! github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da, whose
//! `fp_generic.go` maps Fingerprint32 to farmhashmk's unseeded Hash32.

/*
Fingerprint32 below is FarmHash's (https://github.com/google/farmhash), as
github.com/dgryski/go-farm implements it. Both are published under the MIT License.

FarmHash: Copyright (c) 2014 Google, Inc.
go-farm: Copyright (c) 2014-2017 Damian Gryski
         Copyright (c) 2016-2017 Nicola Asuni - Tecnick.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

const C1: u32 = 0xcc9e2d51;
const C2: u32 = 0x1b873593;

/// Whether this workflow follows the configured ramp, including an unversioned target.
///
/// Nonpositive percentages select current; exactly 100 selects ramping; otherwise
/// compare inclusively with the f32 percentage widened exactly to f64, as in
/// `common/worker_versioning/worker_versioning.go @ v1.31.0`.
pub(super) fn selects_ramping(workflow_id: &str, percentage: f32) -> bool {
    if percentage <= 0.0 {
        false
    } else if percentage == 100.0 {
        true
    } else {
        threshold(workflow_id) <= f64::from(percentage)
    }
}

/// Compute the workflow's threshold in [0, 100), dividing in f64 before multiplying.
///
/// v1.31.0 gives an empty ID a random threshold but refuses it before routing
/// (`common/worker_versioning/worker_versioning.go @ v1.31.0` and
/// `service/frontend/workflow_handler.go @ v1.31.0`); here an empty ID is hashed
/// like any other so every node chooses alike.
pub(super) fn threshold(workflow_id: &str) -> f64 {
    100.0 * (f64::from(fingerprint32(workflow_id.as_bytes())) / (f64::from(u32::MAX) + 1.0))
}

/// Portable FarmHash Fingerprint32 for arbitrary bytes, including zero bytes.
///
/// All arithmetic wraps at 32 bits, including the length, matching go-farm
/// v0.0.0-20240924180020-3414d57e47da independently of host endianness or CPU.
pub(super) fn fingerprint32(mut bytes: &[u8]) -> u32 {
    let len = bytes.len();
    match len {
        0..=4 => {
            let mut b = 0_u32;
            let mut c = 9_u32;
            for &byte in bytes {
                // This branch sign-extends each byte before wrapping to u32;
                // unsigned widening would change UTF-8 and other high-bit inputs.
                b = b
                    .wrapping_mul(C1)
                    .wrapping_add(i32::from(byte as i8) as u32);
                c ^= b;
            }
            return fmix(mur(b, mur(len as u32, c)));
        }
        5..=12 => {
            let a = (len as u32).wrapping_add(fetch32(bytes, 0));
            let d = (len as u32).wrapping_mul(5);
            let b = d.wrapping_add(fetch32(bytes, len - 4));
            let c = 9_u32.wrapping_add(fetch32(bytes, (len >> 1) & 4));
            return fmix(mur(c, mur(b, mur(a, d))));
        }
        13..=24 => {
            let mut a = fetch32(bytes, (len >> 1) - 4);
            let b = fetch32(bytes, 4);
            let c = fetch32(bytes, len - 8);
            let d = fetch32(bytes, len >> 1);
            let e = fetch32(bytes, 0);
            let f = fetch32(bytes, len - 4);
            let mut h = d.wrapping_mul(C1).wrapping_add(len as u32);
            a = a.rotate_right(12).wrapping_add(f);
            h = mur(c, h).wrapping_add(a);
            a = a.rotate_right(3).wrapping_add(c);
            h = mur(e, h).wrapping_add(a);
            a = a.wrapping_add(f).rotate_right(12).wrapping_add(d);
            h = mur(b, h).wrapping_add(a);
            return fmix(h);
        }
        _ => {}
    }

    let mut h = len as u32;
    let mut g = C1.wrapping_mul(len as u32);
    let mut f = g;
    h = mur(fetch32(bytes, len - 4), h);
    h = mur(fetch32(bytes, len - 16), h);
    g = mur(fetch32(bytes, len - 8), g);
    g = mur(fetch32(bytes, len - 12), g);
    f = f
        .wrapping_add(mix_word(fetch32(bytes, len - 20)))
        .rotate_right(19)
        .wrapping_add(113);

    // Hash32 folds the tail above and consumes only blocks with >20 bytes
    // remaining; processing a final exact 20-byte block changes the fingerprint.
    while bytes.len() > 20 {
        let a = fetch32(bytes, 0);
        let b = fetch32(bytes, 4);
        let c = fetch32(bytes, 8);
        let d = fetch32(bytes, 12);
        let e = fetch32(bytes, 16);
        h = h.wrapping_add(a);
        g = g.wrapping_add(b);
        f = f.wrapping_add(c);
        h = mur(d, h).wrapping_add(e);
        g = mur(c, g).wrapping_add(a);
        f = mur(b.wrapping_add(e.wrapping_mul(C1)), f).wrapping_add(d);
        f = f.wrapping_add(g);
        g = g.wrapping_add(f);
        bytes = &bytes[20..];
    }
    g = g.rotate_right(11).wrapping_mul(C1);
    g = g.rotate_right(17).wrapping_mul(C1);
    f = f.rotate_right(11).wrapping_mul(C1);
    f = f.rotate_right(17).wrapping_mul(C1);
    h = h.wrapping_add(g).rotate_right(19);
    h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
    h = h.rotate_right(17).wrapping_mul(C1);
    h = h.wrapping_add(f).rotate_right(19);
    h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
    h.rotate_right(17).wrapping_mul(C1)
}

fn fetch32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("Fingerprint32 branch guarantees a four-byte word"),
    )
}

fn mix_word(value: u32) -> u32 {
    value.wrapping_mul(C1).rotate_right(17).wrapping_mul(C2)
}

fn mur(value: u32, hash: u32) -> u32 {
    (hash ^ mix_word(value))
        .rotate_right(19)
        .wrapping_mul(5)
        .wrapping_add(0xe6546b64)
}

fn fmix(mut hash: u32) -> u32 {
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^ (hash >> 16)
}
