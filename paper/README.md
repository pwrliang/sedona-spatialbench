
```bash
# 1. Create the environment with Postgres 15, PostGIS, and C compilers
conda create --name spatialbench python=3.11

conda activate spatialbench
conda install -c conda-forge postgresql=15 postgis gcc_linux-64 gxx_linux-64 make sysroot_linux-64 arrow-cpp pkg-config libarrow

export CUDA_PATH=/usr/local/cuda
export PATH=$CUDA_PATH/bin:$PATH
export PATH=$CONDA_PREFIX/bin:$PATH
export PG_CONFIG=$CONDA_PREFIX/bin/pg_config
# Make sure you have CUDA and cuobjdump
```


```bash
git clone https://github.com/heterodb/pg-strom.git -b v6.1
cd pg-strom/src

# 1. Locate the stubs directory inside your CUDA path
# This is usually where "nvcc" lives + /lib64/stubs
export CUDA_STUB_DIR="$CUDA_PATH/lib64/stubs"

# Insert Parquet headers immediately after <arrow/api.h>
sed -i '/#include <arrow\/api.h>/a #include <parquet/types.h>\n#include <parquet/schema.h>\n#include <parquet/arrow/reader.h>' arrow_meta.cpp parquet_read.cpp
export DRIVER_DIR=/usr/lib/x86_64-linux-gnu

# 2. Compile with the extra linker path
make SHELL=/bin/bash \
     CFLAGS="-Wno-incompatible-pointer-types -I$CONDA_PREFIX/include -DHAS_LIBARROW=1 -DHAS_PARQUET=1" \
     CXXFLAGS="-I$CONDA_PREFIX/include -DHAS_LIBARROW=1 -DHAS_PARQUET=1" \
     SHLIB_LINK="-L/usr/lib64 -L$DRIVER_DIR -lcuda -L$CONDA_PREFIX/lib -larrow -lparquet -Wl,--rpath,$CONDA_PREFIX/lib" \
     CUDA_NVCC_FLAGS="-gencode arch=compute_86,code=sm_86" \
     -j$(nproc)

# Install into the Conda environment (no sudo required)
make install SHELL=/bin/bash
```

```bash
# 1. Create a local data directory (e.g., in your scratch space)
export PGDATA=$HOME/pgstrom_data
mkdir -p $PGDATA

# 2. Initialize the database cluster
initdb -D $PGDATA

# 3. Configure Postgres to load PG-Strom and PostGIS
# These commands append config lines to your local postgresql.conf
echo "shared_preload_libraries = 'pg_strom'" >> $PGDATA/postgresql.conf

# Allow more background processes for GPU tasks
echo "max_worker_processes = 100" >> $PGDATA/postgresql.conf
echo "shared_buffers = 10GB" >> $PGDATA/postgresql.conf
echo "work_mem = 1GB" >> $PGDATA/postgresql.conf
echo "pg_strom.enabled = on" >> $PGDATA/postgresql.conf
echo "port = 5433" >> $PGDATA/postgresql.conf
```

Start:
```bash
pg_ctl -D $PGDATA -l logfile start

# create database on specific port (-p 5433)
createdb -h 127.0.0.1 -p 5433 spatialbench

# enable extensions
psql -h 127.0.0.1 -p 5433 spatialbench -c "CREATE EXTENSION postgis;"
psql -h 127.0.0.1 -p 5433 spatialbench -c "CREATE EXTENSION pg_strom;"
```
