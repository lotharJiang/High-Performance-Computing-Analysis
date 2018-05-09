#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-01:00:00


# Load required modules
module load Python/2.7.11-goolf-2015a

echo "two nodes with eight cores"
time mpiexec -n 8 python Instagram_Distribution.py
