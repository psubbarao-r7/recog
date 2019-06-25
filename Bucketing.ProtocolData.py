#!/usr/bin/env python
# coding: utf-8


import os
import re
import time
import pandas as pd
import fileinput
import gzip
import sys


sys.path.append('../')
from argparse import ArgumentParser
from logparser import Drain


#Function that finds matches between log contents and corresponding template
def substitute_matches(template, matching, search_strings=["<*>"], verbose=True):
    return_tokens = []
    running_template = template

    # Get a list of search_strings in the order they appear in the template
    # (Note: search_strings are allowed to be repeated)
    found_search_strings = []
    next_found = 0
    while True: # find as many as possible
        any_found = [(ss, running_template.find(ss, next_found)) for ss in search_strings if running_template.find(ss, next_found) > -1]
        if len(any_found) == 0:
            break # didn't find any more of search_strings
        # sort to find the earliest
        any_found_sorted = sorted(any_found, key=lambda x: x[1])
        # take the earliest and repeat
        found_search_strings.append(any_found_sorted[0][0])
        # And then search for any of the search_strings again, but starting after what we just found
        next_found = any_found_sorted[0][1] + 1

    if verbose:
        #print what it's going to look for, in order:
        print("looking for: ")
        #print the template here as well to debug
        print(found_search_strings)
        print("Length : " + str(len(found_search_strings)))

    # Now have an ordered list of search strings to find in the matching string
    # Loop through them, replacing in the template as needed to find the matching string
    for ss in found_search_strings:
        
        ss_start_loc = running_template.find(ss)        
        if verbose:
            print("...Looking for: " + ss)
            print("...found: " + str(ss_start_loc))

        ss_end_loc = ss_start_loc + len(ss)
        ss_end_char = running_template[ss_end_loc:ss_end_loc+1]
        if verbose:
            print("...Ending replacement at: " + str(ss_end_loc))
            print("...Ending replacement char: " + ss_end_char)

        # now find the equivalent in the matching string
        # print("Looking for end with first instance of :" + ss_end_char)
        if ss_end_loc >= len(running_template):  # edge case: if the end of the token is the end of the line
            if verbose:
                print("...Last match going to end of string")
            matching_end_loc = len(matching)
        else:
            matching_end_loc = matching.find(ss_end_char, ss_start_loc)
        if matching_end_loc < 0:
            matching_string = matching[ss_start_loc:]
        else:
            matching_string = matching[ss_start_loc:matching_end_loc]
        return_tokens.append((ss, matching_string))

        if verbose:
            print("...Matching at startloc: " + str(ss_start_loc))
            print("...Matching until endloc: " + str(matching_end_loc))
            print("......is: " + matching_string)
            print()

        # update the template to get rid of the search string we just found
        running_template = running_template[:ss_start_loc] + matching_string + running_template[ss_end_loc:]

    return_token_strings = [tup[0] + " : " + tup[1] for tup in return_tokens]
    return ", ".join(return_token_strings)


if __name__ == "__main__":
    parser = ArgumentParser(description="Templatize log messages")
    parser.add_argument("input_directory", type=str, help="enter the relative path of directory where log files sit awaiting templatization, e.g. ../logsdirectory")
    parser.add_argument("output_directory", type=str, help="relative path of directory where templates are to be written, e.g. ./templatizationoutput")
    parser.add_argument("extraction", nargs='?', type=str, default='extract', help="[optional] should we match the templates with the original log line and extract the output? enter extract; default extract")
    parser.add_argument("similaritythreshold", nargs='?', type=float, default=0.2, help="[optional] tuning parameter, similarity threshold, default is 0.2")
    parser.add_argument("depthnodes", nargs='?', type=int, default=3, help="[optional] tuning parameter, depth of nodes, default is 3")
    args = parser.parse_args()
    input_dir = args.input_directory
    output_dir = args.output_directory
    extract_wildcards = args.extraction
    st = args.similaritythreshold
    depth = args.depthnodes
    log_format = '<Content>' # the <content> section is the portion that gets submitted for templatization; have option to have fields preceeding this section be disregarded
    list_of_all_files = os.listdir(input_dir)
    list_of_all_files.sort()
    regex      = [ # (optional) list of regex; default is []
        r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', # IP
        r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$', # Numbers
    ]
    log_file = 'concatenated_log_file.txt'
    Extraction_Directory = 'ExtractionOutput'


    with open(os.path.join(input_dir, log_file), 'wb') as outfile:
        for fname in list_of_all_files:
            with gzip.open(os.path.join(input_dir, fname), 'rb') as infile:
                for line in infile:
                    outfile.write(line.replace(b"/", b" ").replace(b"\\", b" ").replace(b"(", b" ").replace(b")", b" ").replace(b"[", b" ").replace(b"]", b" ").replace(b".", b" "))


    #Executes Drain
    parser = Drain.LogParser(log_format, indir=input_dir, outdir=output_dir,  depth=depth, st=st, rex=regex)
    parser.parse(log_file)


    structured_file = os.path.join(output_dir, log_file + '_structured.csv')
    template_file = os.path.join(output_dir, log_file + '_templates.csv')


    train = pd.read_csv(structured_file)


    #If extraction is specified by "extract", create file ExtractedContents.txt in output_dir/outputs containing all of the extracted fields from the wildcards
    if extract_wildcards == 'extract':
        extracted_contents_directory = os.path.join(output_dir, Extraction_Directory)
        os.makedirs(os.path.join(output_dir, Extraction_Directory))
        with open(os.path.join(extracted_contents_directory, 'ExtractedContents.txt'), "w") as outfile:
            string_write_out = "" #"Template ID: " + str(train.EventId)
            for i, every_line in train.iterrows():
                line_i = train.LineId
                verbose = False
                matches_i = substitute_matches(str(every_line.EventTemplate), str(every_line.Content), verbose=verbose) + "\n"
                string_write_out += "TemplateID: " + str(every_line.EventId) + ", "
                string_write_out += matches_i
            outfile.write(string_write_out)


    #In the output directory, create a text file for each unique template listing all the log lines that exhibit that template
    for name, dfgroup in train.groupby('EventId'):
        dfgroup[['Content']].to_csv(path_or_buf=os.path.join(output_dir, name + '.csv'), header=False, index=False)

    os.remove(os.path.join(input_dir, log_file))
