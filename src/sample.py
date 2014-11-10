from vkapi import VkAPI
import random
import time
from collections import defaultdict

if __name__ == '__main__':
    api = VkAPI()
        
    random.seed(23912345)
    user_ids = range(1,10000000)
    random.shuffle(user_ids)
    
    cut = lambda ar, n=1000: [ar[i:(i + n)] for i in range(0, len(ar), n)]
    stat = defaultdict(set)
    keys = ['city', 'bdate', 'schools', 'universities', 'home_town']
    all = set()
    
    today = time.time()
    last_threshold = 3600 * 24 * 365
    
    for lst in cut(user_ids):
        all = reduce(lambda x,y: x | y, map(lambda x: stat[x], keys))
        if len([key for key in keys if len(stat[key]) > 5000]) == len(keys):
                break
        print [len(stat[key]) for key in keys], len(all)
                
        profiles = [profile for profile in api.get_user_profiles(lst) 
                        if 'last_seen' in profile and (today - profile['last_seen']['time']) < last_threshold]
                           
        for profile in profiles:              
            user_id = profile['id']
            for key in keys:
                if key in profile and len(profile[key]) > 0:
                    stat[key].add(user_id)
            if user_id in stat['bdate'] and profile['bdate'].count('.') < 2:
                    stat['bdate'].remove(user_id)
     
    with open('samples.csv', 'w') as f:
        f.write('user_id,city,bdate,schools,universities,home_town\n')
        for l in sorted(list(all)):
            f.write('%s,%s\n' % 
                    (l, ','.join([str(int(l in stat[key])) for key in keys])
                    )
            )