import os
import re

all_data = {}

session_id=0
for f in os.listdir("prom_dump/"):
    if "transfered_bytes" in f[:16]:
        for line in open("prom_dump/" + f, 'r').readlines():
            if "session_id" in line:
                session_id_re = re.compile(r".*session_id=\"(\d+)\"")
                session_id = int(re.match(session_id_re, line).groups(0)[0])
                if session_id not in all_data:
                    all_data[session_id] = {}
            elif " @[" in line:
                bytes_transfered, timestamp = line[:-2].split(" @[")
                all_data[session_id][int(timestamp)] = int(bytes_transfered)

elapsed_time_data = {}
session_id=0
for f in os.listdir("prom_dump/"):
    if "elapsed_time" in f[:16]:
        for line in open("prom_dump/" + f, 'r').readlines():
            if "session_id" in line:
                session_id_re = re.compile(r".*session_id=\"(\d+)\"")
                session_id = int(re.match(session_id_re, line).groups(0)[0])
                if session_id not in elapsed_time_data:
                    elapsed_time_data[session_id] = {}
            elif " @[" in line:
                line = line.split("]")[0]
                session_elapsed_time, timestamp = line.split(" @[")
                elapsed_time_data[session_id][int(timestamp)] = int(session_elapsed_time)

t0_data = {}
for session_id, data in all_data.items():
    t0_data[session_id] = {}
    for timestamp, bytes_transfered in data.items():
        try:
            elapsed_time = elapsed_time_data[session_id][int(timestamp)]
            t0_data[session_id][elapsed_time] = bytes_transfered
        except:
            print session_id
            print timestamp
            print elapsed_time_data[session_id]
            quit()

sessions = {}
# data_by_f_size[fsize][read_or_write][time_in_session] = bytes_transferred
data_by_f_size = {}
for f in os.listdir("../renbauer/sessions/"):
    if "l2s" not in f:
        continue
    file_size = int(f.split(".")[1])
    sessions[file_size] = {'write': 0, 'read': 0}
    data_by_f_size[file_size] = {"read": {}, "write": {}}
    for line in open("../renbauer/sessions/%s" % f, 'r').readlines():
        session = int(line.split(" ")[1])
        if session < 8700: # Don't have data for old-old sessions
            continue
        if "read" in line:
            data_by_f_size[file_size]["read"] = t0_data[session]
            sessions[file_size]["read"] = session
        if "write" in line:
            data_by_f_size[file_size]["write"] = t0_data[session]
            sessions[file_size]["write"] = session

# data_by_t[time_in_session][fsize][read_or_write] = bytes_transferred
data_by_t = {}
for f_size, data in data_by_f_size.items():
    for read_or_write, sub_data in data.items():
        for timestamp, bytes_transfered in sub_data.items():
            if timestamp not in data_by_t:
                data_by_t[timestamp] = {}
            if f_size not in data_by_t[timestamp]:
                data_by_t[timestamp][f_size] = {'read': '', 'write': ''}
            data_by_t[timestamp][f_size][read_or_write] = bytes_transfered

output_csv = "file_size,read/write,session_elapsed_time,bytes_transfered\n"
for file_size, data in data_by_f_size.items():
    for read_or_write, sub_data in data.items():
        for elapsed_time, bytes_transfered in sub_data.items():
            output_csv += "%s,%s,%s,%s\n" % (file_size, read_or_write, elapsed_time, bytes_transfered)


output_csv_header = "session_elapsed_time,"
fsize_order = sorted(data_by_f_size.keys())
for f_size in fsize_order:
    output_csv_header += "%sM read (%s),%sM write (%s)," % (f_size / 1000000, sessions[f_size]["read"], f_size / 1000000, sessions[f_size]["write"])

output_csv_data = ""
output_csv_rate_data = ""
last_timestamp_read = {}
last_timestamp_write = {}
# data_rates_by_fsize[fsize][read_or_write] = [rates]
data_rates_by_fsize = dict((fsize, {"read": [], "write": []}) for fsize in fsize_order)
for timestamp in sorted(data_by_t.keys()):
    data = data_by_t[timestamp]
    output_csv_dataline = "%s," % timestamp
    output_csv_rate_dataline = "%s," % timestamp
    for f_size in fsize_order:
        if f_size not in data:
            output_csv_dataline += ",,"
            output_csv_rate_dataline += ",,"
            continue
        f_data = data[f_size]
        output_csv_dataline += "%s,%s," % (f_data["read"], f_data["write"])
        read_rate=''
        write_rate=''
        if timestamp > 0:

            if f_size in last_timestamp_read:
                last_read_val = data_by_t[last_timestamp_read[f_size]][f_size]["read"]
                if f_data["read"] != '' and f_data["read"] > last_read_val:
                    last_timestamp_read[f_size] = timestamp
                    read_rate = f_data["read"] / timestamp
                    if timestamp > 2000:
                        data_rates_by_fsize[f_size]["read"].append(read_rate)
            elif f_data["read"] != '':
                last_timestamp_read[f_size] = timestamp

            if f_size in last_timestamp_write:
                last_write_val = data_by_t[last_timestamp_write[f_size]][f_size]["write"]
                if f_data["write"] != '' and f_data["write"] > last_write_val:
                    last_timestamp_write[f_size] = timestamp
                    write_rate = f_data["write"] / timestamp
                    if timestamp > 2000:
                        data_rates_by_fsize[f_size]["write"].append(write_rate)
            elif f_data["write"] != '':
                last_timestamp_write[f_size] = timestamp

            output_csv_rate_dataline += "%s,%s," % (read_rate, write_rate)
    output_csv_data += output_csv_dataline + "\n"
    if timestamp > 0:
        output_csv_rate_data += output_csv_rate_dataline + "\n"

# avg_rate_by_f_size[fsize][read_or_write] = avg_rate
# avg_rate_by_fsize = {}
output_csv_avg_rate_header = "fsize (MB), avg_read, avg_write"
output_csv_avg_rate_data = ""
for fsize in fsize_order:
    output_csv_avg_rate_data += "%s," % (int(fsize) / 1000000)
    for read_or_write in ["read", "write"]:
        rates = data_rates_by_fsize[fsize][read_or_write]
        avg_rate = ''
        if len(rates) == 0:
            print "0 rates for fsize: %sMB (%s)" % (int(fsize) / 1000000, read_or_write)
        else:
            avg_rate = sum(rates) / len(rates) / 1000000
        output_csv_avg_rate_data += "%s," % avg_rate
    output_csv_avg_rate_data += "\n"

output = open('bytes_transfered_by_t.csv', 'w')
output.write("\n".join([output_csv_header, output_csv_data]))
output.close()

output = open('bytes_transfered_rates_by_t.csv', 'w')
output.write("\n".join([output_csv_header, output_csv_rate_data]))
output.close()

output = open('bytes_transfered_avg_rates_by_fsize.csv', 'w')
output.write("\n".join([output_csv_avg_rate_header, output_csv_avg_rate_data]))
output.close()
