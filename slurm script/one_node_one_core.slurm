#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-01:00:00


# Load required modules
module load Python/2.7.11-goolf-2015a

echo "one node with one core"
time mpiexec -n 1 python Instagram_Distribution.py