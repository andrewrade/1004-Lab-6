from mrjob.job import MRJob
from datetime import datetime
import time

class MRPrecipJoin(MRJob):
    '''
    Class to join LAX-JFK data with LAX hourly precip data
    '''
    def mapper(self, _, line):
        
        fields = line.split(',')
        
        try:
        # Precip data mapper (precip data contains exactly 4 fields)
            if len(fields) == 4:
                _, _, date_str, precip = fields
                date = datetime.strptime(date_str, '%Y%m%d %H:%M')
                key = date.strftime('%Y-%m-%d %H')
                value = ('precip', precip)
                yield key, value
            # Flight data mapper 
            elif len(fields) > 4:
                flight_date_str, dep_time, dep_delay = fields[2], fields[6], fields[18]
                
                # Strip minutes from flight dep time and join w/ nearest hourly precip data
                flight_date = datetime.strptime(flight_date_str + dep_time, '%m/%d/%Y%H%M')
                flight_date = flight_date.replace(minute=0)
                
                key = flight_date.strftime('%Y-%m-%d %H')
                value = ('flight', (dep_time, dep_delay))
                yield key, value
        except:
            pass
    
    def reducer(self, key, values):
        flights = []
        precip = None

        for value_source, value in values:
            if value_source == 'flight':
                flights.append(value)
            else:
                precip = float(value)


        for flight in flights:
            # Key: YYYY/MM/DD H; Value: departure time (HH:MM), departure delay (min), precip (in)
            yield key, (flight[0], flight[1], precip) 

if __name__ == '__main__':
    
    start = time.time()
    MRPrecipJoin.run()
    end = time.time()
    print(f"Elapsed Time: {end-start}")