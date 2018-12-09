from ipfs_logparse import Bandwidth
import os.path
import csv
import shutil
import io

PATH_TO_LOGS = "/Users/nhf/Projects/6s974/ipfs/logs/all-logs/"
OUTPUT_FN="bandwidth.summary.csv"
HEADER = ('protocol', 'timestamp', 'total_down', 'total_up', 'rate_in', 'rate_out')

def parserToCSV(parser, output):
	"""Write parser output to a file handle."""
	print("Generating CSV file")
	writer = csv.DictWriter(output, fieldnames=header
	writer.writerows(parser.output))
	
# Create a stringIO object to collect CSV output from multiple writers
output = io.StringIO()
output.writelines([HEADER])

# Parse each log
for fn in glob(os.path.join(PATH_TO_LOGS, "bandwidth*.log")):
    print(fn)
    parser = Bandwidth(fn)
    parser.parse()
	parserToCSV(parser, output)

# Output
print("Writing CSV file.")	
with open(OUTPUT_FN, 'w') as f:
    output.seek(0)
    shutil.copyfileobj(output, f)
	