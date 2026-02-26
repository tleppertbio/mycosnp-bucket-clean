#!/usr/bin/env python3
# Tami Leppert
# 5/5/2025
# v 1.0
# 1/24/2026 more robust
# v 2.0
#
# fin contains one filename per line from bucket
#
# program mycosnp-bucket-clean.py reads in input file
# checks to make sure each SRR has a .maple, .g.vcf.gz, .done, .finished
# if all 4 files are present, then a script is written to process them
# through the program clean-bucket-rm-vm.script
# If just .bam and .bai and execute-vm2-SRR*.script exists, requeue it
#

import subprocess
import shlex
import sys
from datetime import datetime
import os

############### begin subprocess to write bucket files to output file bucket.list ###################

bucket_filename = "bucket.list"

# gsutil ls command and the bucket path
bucket_path = "gs://test-154312-data-bucket/"
command_str = f"gsutil ls {bucket_path}"

# For commands with arguments, it is generally safer to pass them as a list of strings
# using shlex.split() to correctly handle spaces and quotes.
command_list = shlex.split(command_str)

try:
    # Use subprocess.run for simple command execution
    # capture_output=True captures stdout and stderr
    # text=True ensures output is returned as a string rather than bytes (Python 3.7+)
    result = subprocess.run(
        command_list,
        capture_output=True,
        text=True,
        check=True # check=True raises an exception if the command fails
    )

    #print("Command executed successfully. Output:")
    # The standard output (list of files/buckets) is stored in result.stdout
    #print(result.stdout)
    output = result.stdout
    # Write the captured output to a file
    with open(bucket_filename, "w") as f:
        f.write(output)
        print(f"Successfully wrote output to '{bucket_filename}'")

except subprocess.CalledProcessError as e:
    print(f"Command failed with exit code {e.returncode}")
    print(f"Error output (stderr): {e.stderr}")
except FileNotFoundError:
    print("Error: gsutil command not found.")
    print("Please ensure Google Cloud SDK is installed and configured in your system's PATH.")

# Using shell=True (less secure, but powerful)
subprocess.run("gsutil ls gs://test-154312-data-bucket/ > bucket.list", shell=True)

############### end subprocess to write bucket files to output file bucket.list ###################

############### begin subprocess to cp bucket files to local directory  ###################

def gs_cp_command(file_from, file_to):
    """
    Copies files from/to Google Cloud Storage using gsutil cp.
    Args:
    source (str): Source file or gs:// path.
    destination (str): Destination file or gs:// path.
    """
    #cmd = ["gsutil"]
    #cmd.append("cp")
    #cmd.extend([file_from, file_to])
    cmd = f"gsutil cp {file_from} {file_to}"

    try:
        #print(f"Executing: {' '.join(cmd)}")
        # Run command
        result=subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"Copy {file_from} successful.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error copying file {file_from}: {e.stderr}", file=sys.stderr)
        #raise
        return False

############### end subprocess to cp bucket files to local directory  ###################    

############### begin subprocess to rm bucket files  ###################

def gs_rm_command(file_to_remove):
    """
    rm file from Google Cloud Storage using gsutil rm.
    Args:
    source (str): gs:// path.
    """
    #cmd = ["gsutil","rm"]
    #cmd.append([file_to_remove])
    cmd = f"gsutil rm {file_to_remove}"    

    try:
        # Run command
        result=subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"Successfully deleted: {file_to_remove}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error deleting {file_to_remove}: {e.stderr}", file=sys.stderr)
        return False

############### end subprocess to rm bucket file  ###################


##########################################
############### BEGIN MAIN ###############
##########################################

# Find current path
from pathlib import Path
current_directory = Path.cwd()
print(current_directory)

################ open input and output files  #######################
# Bucket input file, lists files in the bucket
fin = open(bucket_filename, 'r')

# Cleanup samples that have run
fileout = "cleanup-mycosnp-vm.script"
fout = open(fileout, 'w')

# Use timestamp to log when files were cleaned up
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filelog = f"cleanup-bucket_{timestamp}.log"
file_content=f"This file was created at {timestamp}."
flog = open(filelog,'w')
flog.write(f"{file_content}\n")

############# end of open input and output files  ###################


trimmed = 0    # first if not-preemptible or vm1, prev exists if vm2
fastp = 0      # first if not-preemptible or vm1, prev exists if vm2
early_bam = 0  # second if not-preemptible or vm1, prev exists if vm2
bam = 0        # third if not-preemptible or vm1, prev exists if vm2
bai = 0        # third if not-preemptible or vm1, prev exists if vm2
done = 0       # fourth if not-preemptible or vm1 flags .vm1.done, first if vm2
vcf = 0        # fourth if not-preemptible or vm1, first if vm2
maple = 0      # fourth if not-preemptible or vm1, first if vm2
vm1_done = 0   # fourth if vm1
finished = 0   # fifth if not-preemptible or vm1, second if vm2

last_SRR = ""  # Track the last_SRR number to know when you move to a new sample

# for each line in the bucket file read list of files in the bucket
for line in fin:

    # debug print("line: " + line.strip())
    # find the SRR number
    columns = line.strip().split('/')

    # get the sra string put it into srr_number
    SRR_number = columns[3].split('.')

    #debug print("SRR_number : " + SRR_number[0] )
    #debug print("last_SRR : " + last_SRR )    
    if ( last_SRR == "" ):  # for initial setup.
        last_SRR = SRR_number[0]

    # If you are at a new sample, then process what you've found about the old sample
    if ( last_SRR != SRR_number[0] ):
        
        # Did you finished processing sample to end?
        if ( done and maple and vcf and finished ):
            #clean up .finished and vm instances
            fout.write(f"{current_directory}/reset.script done {last_SRR}\n")
            #rm .done
            rmfile=f"gs://test-154312-data-bucket/{last_SRR}.done"
            if gs_rm_command(rmfile):
                flog.write(f"removed {last_SRR}.done from bucket\n")
                
            #check if current_directory/maple folder exists, if not create it.
            filepath=f"{current_directory}/maple"
            maple_exists=Path(filepath)
            maple_exists.mkdir(parents=True,exist_ok=True)

            #copy maple file from bucket to folder in current directory
            fromfile=f"gs://test-154312-data-bucket/{last_SRR}.maple"
            tofile=f"{current_directory}/maple/{last_SRR}.maple"
            if gs_cp_command(fromfile,tofile):
                gs_rm_command(fromfile)
                flog.write(f"moved {last_SRR}.maple from bucket to /maple\n")

            #copy .g.vcf.gz file from bucket to folder in current directory                
            fromfile=f"gs://test-154312-data-bucket/{last_SRR}.g.vcf.gz"
            tofile=f"{current_directory}/maple/{last_SRR}.g.vcf.gz"
            if gs_cp_command(fromfile,tofile):
                gs_rm_command(fromfile)
                flog.write(f"moved {last_SRR}.g.vcf.gz from bucket to /maple\n")

            #Clean up rest of files from bucket for this sample
            if bam == 1:
                rmfile=f"gs://test-154312-data-bucket/{last_SRR}.bam"
                if gs_rm_command(rmfile):
                    flog.write(f"removed {last_SRR}.bam from bucket\n")
            if bai == 1:
                rmfile=f"gs://test-154312-data-bucket/{last_SRR}.bai"
                if gs_rm_command(rmfile):
                    flog.write(f"removed {last_SRR}.bai from bucket\n")
            if early_bam == 1:
                rmfile=f"gs://test-154312-data-bucket/{last_SRR}.early.bam"
                if gs_rm_command(rmfile):
                    flog.write(f"removed {last_SRR}.early.bam from bucket\n")                    
            if vm1_done == 1:
                rmfile=f"gs://test-154312-data-bucket/{last_SRR}.vm1.done"
                if gs_rm_command(rmfile):
                    flog.write(f"removed {last_SRR}.vm1.done from bucket\n")
            if fastp == 1:
                rmfile=f"gs://test-154312-data-bucket/{last_SRR}.fastp.log"
                if gs_rm_command(rmfile):
                    flog.write(f"removed {last_SRR}.fastp.log from bucket\n")
            if trimmed > 0:
                rmfile=f"gs://test-154312-data-bucket/{last_SRR}.1.trimmed.fastq"
                if gs_rm_command(rmfile):
                    flog.write(f"removed {last_SRR}.1.trimmed.fastq from bucket\n")
                rmfile=f"gs://test-154312-data-bucket/{last_SRR}.2.trimmed.fastq"
                if gs_rm_command(rmfile):
                    flog.write(f"removed {last_SRR}.2.trimmed.fastq from bucket\n")
                    

        # Else if the current sample finished the first half of the process, vm1, then start the next half
        elif ( bam and bai and vm1_done and finished ):
            # fout.write(current_directory + "/queue-vm2.script " + last_SRR + "\n")
            fout.write(f"{current_directory}/reset.script vm2 {last_SRR}\n")            
        # .bai and .bam only done, can startup vm using these files, no need to recalculate them
        elif ( not done and not maple and not vcf and bam and bai and not vm1_done and finished ):
            fout.write(f"{current_directory}/reset.script finalbam {last_SRR}\n")
        # .early.bam only done, can startup vm using this file, no need to recalculate it
        elif ( not done and not maple and not vcf and not bam and not bai and not vm1_done and finished and early_bam):
            fout.write(f"{current_directory}/reset.script earlybam {last_SRR}\n")
        # trimmed only done, can startup vm using these files, no need to recalculate them
        elif ( not done and not maple and not vcf and not bam and not bai and not early_bam and (trimmed>0) and finished ):
            fout.write(f"{current_directory}/reset.script trimmed {last_SRR}\n")
        # only finished nothing else - failed miserably - need to rerun
        elif ( not done and not maple and not vcf and not bam and not bai and not early_bam and (trimmed == 0) and finished ):
            fout.write(f"{current_directory}/reset.script failed {last_SRR}\n")
        else:
            flog.write(f"echo '{last_SRR}' not finished.\n")     

        # Reset what you've learned about the last sample, ready for next sample
        done = 0
        finished = 0
        maple = 0
        fastp = 0
        vcf = 0
        bam = 0
        bai = 0
        early_bam = 0
        vm1_done = 0
        trimmed = 0
        last_SRR = SRR_number[0]
        
    #end of if ( last_SRR != SRR_number[0] ):        

    # Set file flags for the new sample
    if ( ".trimmed" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        if (trimmed == 1):
            trimmed = 2  # second one was found
        else:
            trimmed = 1
        #debug print("trimmed : " + columns[3] )                        
    if ( ".early.bam" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        early_bam = 1
    if ( ".bam" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        bam = 1
    if ( ".bai" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        bai = 1
    if ( ".vm1.done" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        vm1_done = 1
    elif ( ".done" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        done = 1
    if ( ".maple" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        maple = 1
    if ( ".vcf" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        vcf = 1
    if ( ".fastp" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        fastp = 1        
    if ( ".finished" in columns[3] ) and ( SRR_number[0] in columns[3] ):
        finished = 1
#end of for line in fin: read next file from bucket

# At the end of the bucket file, proccess the last last_SRR
# Did you finished processing sample to end?
if ( done and maple and vcf and finished ):
    #clean up .finished and vm instances
    fout.write(f"{current_directory}/reset.script done {last_SRR}\n")
    #rm .done
    rmfile=f"gs://test-154312-dafta-bucket/{last_SRR}.done"
    if gs_rm_command(rmfile):
        flog.write(f"removed {last_SRR}.done from bucket\n")

    #check if current_directory/maple exists, if not create it.
    filepath=f"{current_directory}/maple"
    maple_exists=Path(filepath)
    maple_exists.mkdir(parents=True,exist_ok=True)

    #copy maple file from bucket to folder in current directory    
    fromfile=f"gs://test-154312-data-bucket/{last_SRR}maple"
    tofile=f"{current_directory}/maple/{last_SRR}.maple"
    if gs_cp_command(fromfile,tofile):
        if gs_rm_command(fromfile):
            flog.write(f"moved {last_SRR}.maple from bucket to /maple\n")

    #copy .g.vcf.gz file from bucket to folder in current directory                
    fromfile=f"gs://test-154312-data-bucket/{last_SRR}.g.vcf.gz"
    tofile=f"{current_directory}/maple/{last_SRR}.g.vcf.gz"
    if gs_cp_command(fromfile,tofile):
       if gs_rm_command(fromfile):
           flog.write(f"moved {last_SRR}.g.vcf.gz from bucket to /maple\n")

    #Clean up rest of files from bucket for this sample           
    if bam == 1:
        rmfile=f"gs://test-154312-data-bucket/{last_SRR}.bam"
        if gs_rm_command(rmfile):
            flog.write(f"removed {last_SRR}.bam from bucket.\n")        
    if bai == 1:
        rmfile=f"gs://test-154312-data-bucket/{last_SRR}.bai"
        if gs_rm_command(rmfile):
            flog.write(f"removed {last_SRR}.bai from bucket.\n")                    
    if early_bam == 1:
        rmfile=f"gs://test-154312-data-bucket/{last_SRR}.early.bam"
        if gs_rm_command(rmfile):
            flog.write(f"removed {last_SRR}.early.bam from bucket.\n")   
    if vm1_done == 1:
        rmfile=f"gs://test-154312-data-bucket/{last_SRR}.vm1.done"
        if gs_rm_command(rmfile):
            flog.write(f"removed {last_SRR}.vm1.done from bucket.\n")               
    if fastp == 1:
        rmfile=f"gs://test-154312-data-bucket/{last_SRR}.fastp.log"
        if gs_rm_command(rmfile):
            flog.write(f"removed {last_SRR}.fastp.log from bucket.\n")               
    if trimmed > 0:
        rmfile=f"gs://test-154312-data-bucket/{last_SRR}.1.trimmed.fastq"
        if gs_rm_command(rmfile):
            flog.write(f"removed {last_SRR}.1.trimmed.fastq from bucket.\n")
        rmfile=f"gs://test-154312-data-bucket/{last_SRR}.2.trimmed.fastq"
        if gs_rm_command(rmfile):
            flog.write(f"removed {last_SRR}.2.trimmed.fastq from bucket.\n")            

# Else if the current sample finished the first half of the process, vm1, then start the next half
elif ( done and bam and bai and vm1_done and finished ):
    # fout.write(current_directory + "/queue-vm2.script " + last_SRR + "\n")
    fout.write(f"{current_directory}/reset.script vm2 {last_SRR}\n")            
# .bai and .bam only done, can startup vm using these files, no need to recalculate them
elif ( not done and not maple and not vcf and bam and bai and not vm1_done and finished ):
    fout.write(f"{current_directory}/reset.script finalbam {last_SRR}\n")
# .early.bam only done, can startup vm using this file, no need to recalculate it
elif ( not done and not maple and not vcf and not bam and not bai and not vm1_done and finished and early_bam):
    fout.write(f"{current_directory}/reset.script earlybam {last_SRR}\n")
# trimmed only done, can startup vm using these files, no need to recalculate them
elif ( not done and not maple and not vcf and not bam and not bai and not early_bam and (trimmed>0) and finished ):
    fout.write(f"{current_directory}/reset.script trimmed {last_SRR}\n")
# only finished nothing else - failed miserably
elif ( not done and not maple and not vcf and not bam and not bai and not early_bam and (trimmed == 0) and finished ):
    fout.write(f"{current_directory}/reset.script failed {last_SRR}\n")
else:
    flog.write(f"echo '{last_SRR}' not finished.\n")     
#end of ifs (for last last_SRR)

# chmod 755 fileout
os.chmod(fileout,0o775)

# close files
fin.close()
fout.close()
flog.close()
