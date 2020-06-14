#!/usr/bin/python3
import random
import names
import os
import argparse
import uuid
import concurrent.futures

'''
   This script generates random data for map reduce of n records
   n is user provided value in command line.
   The output is save into a file provided by user.

   Output: CSV file with
     id, name, taxamount, year, zipcode, household
'''

################################################################
#                   GLOBAL DATA                                #
################################################################
ZIPCODES = []
ZIPCODES_LEN = 0
NUM_TASKS = 10
NUM_WORKERS = 8
BASE_YEAR = 2000
SEED_FILES = []
DERIVED_START_YEAR = BASE_YEAR + 1

################################################################
#                   INITIALIZE METHODS                         #
################################################################
def load_zipcodes(data_file):
   """
   Load zipcodes from the data_file.
   """
   global ZIPCODES
   file_handle = open(data_file, "r")
   
   # read file line by line.
   for line in file_handle:
      ZIPCODES.append(line.strip())
   ZIPCODES_LEN = len(ZIPCODES)
   file_handle.close()


def initialize(input_file, output_dir, num_records, seed_dir=None, base_year=2000):
   """
   Perform various initialization like
       - Loading zipcodes
       - Seeding random number generator
       - Cleaning out dir.
   """
   global ZIPCODES
   global ZIPCODES_LEN
   global NUM_TASKS
   global SEED_FILES
   global BASE_YEAR
   global DERIVED_START_YEAR

   BASE_YEAR = base_year
   DERIVED_START_YEAR = BASE_YEAR + 1

   random.seed()
   load_zipcodes(input_file)
   ZIPCODES_LEN = len(ZIPCODES)

   if seed_dir:
      if not os.path.exists(seed_dir):
         print("Seed directory not found")
      else:
         DERIVED_START_YEAR = BASE_YEAR
         for f in os.listdir(seed_dir):
            SEED_FILES.append(os.path.join(seed_dir, f))
   
   if len(SEED_FILES) > 0:
      NUM_TASKS = len(SEED_FILES)
   else:
      if num_records >= 10000:
         NUM_TASKS = 100
                
   if os.path.exists(output_dir):
      print("Removing the contents of the directory {}".format(output_dir))
      for f in os.listdir(output_dir):
         os.remove("{}/{}".format(output_dir, f))
      print("Cleanup done.")
   else:
      os.mkdir(output_dir)


################################################################
#                   HELPER METHODS                             #
################################################################
def get_random_indices(cnt, num_records):
   """
   Get random cnt number of indices in range (0, num_records)
   """
   index_values = set()

   while len(index_values) != cnt:
      random_index = random.randrange(num_records)
      index_values.add(random_index)
      
   return list(index_values)


################################################################
#                   RECORD GENERATION METHODS                  #
################################################################
def generate_record(year, should_generate_null=False):
   "Generates a single random record"
   global ZIPCODES

   record = []
   random_zipcode_index = random.randrange(ZIPCODES_LEN)
   random_amount = random.randrange(100, 100000)
   random_household = random.randrange(1500, 15000)
   
   # add random ID 
   record.append(str(uuid.uuid4()))

   # add random name
   record.append(names.get_first_name() )

   # add random tax amount 
   record.append(str(random_amount))

   # add year which is static for some part of data
   record.append(year)

   # add zicode
   record.append(ZIPCODES[random_zipcode_index])

   # add household
   record.append(str(random_household))

   if should_generate_null:
      random_null_index = random.randrange(0, 6)
      record[random_null_index] = ""

   return ",".join(record)


def get_new_records(record_cnt, year, should_have_dirty_record=False, dirty_record_percent=20):
   records = []
   if should_have_dirty_record:
      dirty_record_cnt = (int) ((record_cnt * dirty_record_percent)/100)
      dirty_record_indices = get_random_indices(dirty_record_cnt, record_cnt)
      i = 0
      while(i < record_cnt):
         record = None
         if i in dirty_record_indices:
            record = generate_record(str(year), True)
         else:
            record = generate_record(str(year), False)
      
         records.append(record)
         i += 1
   else:
      for _ in range(record_cnt):
         records.append(generate_record(year, False))
   return records


def generate_and_write_new_records_to_file(file_handle, num_records, year, should_have_dirty_record, dirty_record_percent):
   records = []
   had_written_before = False
   while(num_records >= 1000):
      #get and save 1000 records at a time.
      records = get_new_records(1000, year, should_have_dirty_record, dirty_record_percent)
      num_records -= 1000
      file_handle.write("\n".join(records))
      had_written_before = True
      records = []
   
   if num_records > 0:
      if had_written_before:
         file_handle.write("\n")
      records = get_new_records(num_records, year, should_have_dirty_record, dirty_record_percent)
      file_handle.write("\n".join(records))
   
   file_handle.write("\n")
   

def record_generator_task(num_records, output_file, year, should_have_dirty_record, dirty_record_percent):
   file_handle = open(output_file, "a")
   generate_and_write_new_records_to_file(file_handle, num_records, year, should_have_dirty_record, dirty_record_percent)
   file_handle.close()


def generate_base_record(num_records, out_dir, year=2000, should_have_dirty_record=False, dirty_record_percent=20):
   global NUM_TASKS
   global NUM_WORKERS
   
   num_records_per_task = (int) (num_records / NUM_TASKS)
   trailing_record_cnt = num_records % NUM_TASKS
   base_file_list = []

   print("Creating worker {} tasks.".format(NUM_TASKS))
   print("Each task will produce {} number of records".format(num_records_per_task))
   with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
      for i in range(NUM_TASKS):
         task_out_file = "{}/{}_{}.csv".format(out_dir, year, i)
         base_file_list.append(task_out_file)
         if i == NUM_TASKS - 1:
            num_records_per_task += trailing_record_cnt

         executor.submit(record_generator_task, num_records_per_task, task_out_file, str(year), should_have_dirty_record=True, dirty_record_percent=20)
         # most of the files will have this number of records except last one.
         # this field will be used for deriving new records.
         
      num_records_per_task -=trailing_record_cnt
   
   return num_records_per_task, base_file_list


def generate_derived_records_task(num_records, year, base_file, base_record_cnt, output_file, new_record_percent=20, should_have_dirty_record=False, dirty_record_percent=20):
   new_records_count = (int)((num_records * new_record_percent)/100)
   output_file_handle = open(output_file, "w")
   skip_list = get_random_indices(new_records_count, base_record_cnt)

   generate_and_write_new_records_to_file(output_file_handle, new_records_count, str(year), should_have_dirty_record, dirty_record_percent)

   record_num = 0
   records = []
   base_file_handle = open(base_file, "r")
   for record_str in base_file_handle:
      record_str = record_str.strip()
      if record_num in skip_list:
         record_num += 1
         continue
      
      record = record_str.split(",")
      tax_amt = str(random.randrange(100, 100000))
      household = random.randrange(1500, 15000)
      record[2] = tax_amt
      record[3] = str(year)
      record[5] = str(household)
      records.append(",".join(record))
   
      if len(records) >= 500:
         output_file_handle.write("\n".join(records))
         output_file_handle.write("\n")
         records = []
      record_num += 1

      if record_num >= num_records:
         break

   if records:
      output_file_handle.write("\n".join(records))
      output_file_handle.write("\n")
   base_file.close()
   output_file_handle.close()


def create_derived_records(num_years, start_year, num_records_per_year, base_files, num_records_per_base_file, out_dir):
   # Generate records for n years
   global NUM_WORKERS
   global NUM_TASKS
   
   num_records_per_task = (int) (num_records_per_year / NUM_TASKS)
   trailing_record_cnt = num_records_per_year % NUM_TASKS

   with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
      for y in range(0, num_years):
         year = start_year + y
         for i in range(NUM_TASKS):
            if i == NUM_TASKS - 1:
               num_records_per_task += trailing_record_cnt
            out_file_path = "{}/{}_{}.csv".format(out_dir, year, i)
            executor.submit(generate_derived_records_task, num_records_per_task, year, base_files[i], 
                            num_records_per_base_file, out_file_path, new_record_percent=20, 
                            should_have_dirty_record=True, dirty_record_percent=20)
         num_records_per_task = (int) (num_records_per_year / NUM_TASKS)
   
   

################################################################
#                         MAIN                                 #
################################################################
def main(parser):
   global BASE_YEAR
   global SEED_FILES
   global DERIVED_START_YEAR

   args = parser.parse_args()
   num_records = args.num_records
   num_years = args.num_years
   out_dir = args.out_dir
   seed_dir = args.seed_dir
   base_year = args.base_year

   initialize(args.zipcode_file, out_dir, num_records, seed_dir, base_year)

   if num_records <= 0:
      print("No records to produce!")
      exit(0)
   
   if seed_dir:
      print("Skipping base record generation.")
      base_file_list = SEED_FILES
      num_records_per_base_file = len(open(SEED_FILES[0], "r").readlines())
   else:
      print("Generating base record")
      num_records_per_base_file, base_file_list = generate_base_record(num_records, out_dir, year=BASE_YEAR,  should_have_dirty_record=False, dirty_record_percent=20)
      print("Done generating base record.")
      if args.base_only:
         print("Not generating derived records as you have only selected --base-only")
         exit(0)
      num_years -= 1
      
   
   if num_years >= 1:
      print("Creating derived records")
      create_derived_records(num_years, DERIVED_START_YEAR, num_records, base_file_list, num_records_per_base_file, out_dir)
      print("Done creating derived records")


if __name__ == "__main__":

   parser = argparse.ArgumentParser(description="Generate random tax data.")
   group = parser.add_mutually_exclusive_group()

   parser.add_argument("-z", dest="zipcode_file", action="store", help="file zip codes.", required=True)
   parser.add_argument("-o", dest="out_dir", action="store", 
                       help="Output directory where data will be generated in csv format", required=True)
   parser.add_argument("-n", dest="num_records", type=int, action="store", 
                       help="Num records per year", required=True)
   parser.add_argument("--seed-dir", dest="seed_dir", required=False)

   parser.add_argument("--base-year", dest="base_year", type=int, required=False, default=2001)
   
   group.add_argument("--num-years", dest="num_years", type=int, action="store", 
                       help="Total number of years for which to generate records. Year will start from 2000")
   group.add_argument("--base-only", dest="base_only", action="store_true", help="when passed only base data shall be generated.")
   
   main(parser)
