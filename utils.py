import json
def config_file_parser():
    with open('config.json') as json_data_file:
        data = json.load(json_data_file)
    return data

def pretty_time(time_in_sec):
  if time_in_sec < 60:
    return "{} seconds".format(time_in_sec)
  elif time_in_sec < 60 * 60:
    minutes = int(time_in_sec / 60)
    seconds = time_in_sec % 60
    return "{} minutes and {} seconds".format(minutes,seconds)
  else:
    hours = int(time_in_sec / 3600)
    minutes = int((time_in_sec - hours * 3600 ) / 60)
    seconds = time_in_sec % 60
    return "{} hours, {} minutes and {} seconds".format(hours,minutes,seconds)


#data = {}
#data['reddit'] = []
#data['reddit'].append({
#    'client_id': 'xxx',
#    'client_secret': 'xxx',
#    'username': 'xxx',
#    'password': 'xxx'
#})
#with open('config.txt', 'w') as outfile:
#    json.dump(data, outfile)