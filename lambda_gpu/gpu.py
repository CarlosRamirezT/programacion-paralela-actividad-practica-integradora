import os
import numpy as np
from numba import njit, prange

# Intentar importar CUDA
try:
    from numba import cuda
    CUDA_AVAILABLE = cuda.is_available()
except Exception:
    CUDA_AVAILABLE = False

@njit(parallel=True, fastmath=True)
def _normalize_cpu(x):
    mn = x.min()
    mx = x.max()
    rng = mx - mn if mx != mn else 1.0
    out = np.empty_like(x)
    for i in prange(x.size):
        out[i] = (x[i] - mn) / rng
    return out

def _normalize_gpu_numba(x):
    # Kernel simple: y = (x - min) / (max - min)
    # Para demo, calculamos min/max en CPU; luego kernel aplica transformación
    mn = np.min(x)
    mx = np.max(x)
    rng = mx - mn if mx != mn else 1.0

    d_x = cuda.to_device(x)
    d_y = cuda.device_array_like(x)

    threads_per_block = 256
    blocks = (x.size + threads_per_block - 1) // threads_per_block

    @cuda.jit
    def kernel_norm(inp, outp, mn_, rng_):
        i = cuda.grid(1)
        if i < inp.size:
            outp[i] = (inp[i] - mn_) / rng_

    kernel_norm[blocks, threads_per_block](d_x, d_y, mn, rng)
    return d_y.copy_to_host()

def normalize_gpu_aware(arr, simulation=False):
    """
    Normaliza arr a [0,1]. Si hay CUDA y no estamos simulando, usa GPU.
    En caso contrario, usa CPU paralela.
    """
    if (not simulation) and CUDA_AVAILABLE and arr.size > 0:
        try:
            out = _normalize_gpu_numba(arr.astype(np.float32))
            return out, "gpu-cuda-numba"
        except Exception:
            pass
    # Fallback CPU (OpenMP-like via numba parallel)
    out = _normalize_cpu(arr.astype(np.float32))
    return out, "cpu-parallel"
