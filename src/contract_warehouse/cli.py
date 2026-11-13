import argparse
import json
import sqlite3
from .build import build

def main():
    p=argparse.ArgumentParser(description='Build ODS/dimension/fact/ADS service contract warehouse')
    p.add_argument('--source',required=True);p.add_argument('--database',required=True)
    a=p.parse_args()
    try: result=build(a.source,a.database)
    except (ValueError,OSError,sqlite3.Error,OverflowError) as e:p.exit(2,str(e)+'\n')
    print(json.dumps(result,sort_keys=True))
if __name__=='__main__':main()
