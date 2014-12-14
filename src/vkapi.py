import time
import logging
import requests
import codecs
import os
import sys
import json

logger = logging.getLogger(__name__)

class VkError(Exception):    
    pass


PROFILE_FIELDS = ','.join(['nickname', 'screen_name', 'sex', 'bdate', 'city', 'relation', 'country', 'education', 'counters', 'home_town',
                               'universities', 'schools', 'connections', 'relation', 'relatives', 'interests', 'books', 'last_seen', 'occupation'])    
    
class VkAPI(object):    
         
    def __init__(self, token=None):
        self.session = requests.Session()
        self.session.headers['Accept'] = 'application/json'
        self.session.headers['Content-Type'] = 'application/x-www-form-urlencoded'
        
        self.token = token
        self.requests_times = []
        
    def _do_api_call(self, method, params):
        self._pause_before_request()
        
        if self.token:
            params['access_token'] = self.token
        params['v'] = '5.26'
            
        param_str = '&'.join(['%s=%s' % (k, v) for k, v in params.iteritems()])
        url = 'https://api.vk.com/method/%s?%s' % (method, param_str)
        logger.debug('API request: %s' % (method))
        
        response = self.session.get(url)
        if response.status_code is not 200:
            time.sleep(10)
            response = self.session.get(url)
            if response.status_code is not 200:
                raise VkError('Can\'t get %s, code %s' % (url, response.status_code))        
                
        json = response.json()
        if 'response' not in json:
            raise VkError('Api call error %s - %s' % (url, json))
        
        return json['response'] 
        
    def _pause_before_request(self):
        if len(self.requests_times) > 2:
            first = self.requests_times[0]
            diff = time.time() - first
            if diff < 1.:
                logger.info('Sleepping for %s sec' % (1. - diff))
                time.sleep(1.- diff)
            self.requests_times = self.requests_times[1:]            
        self.requests_times.append(time.time())
        
    def get_user_profile(self, user_id, fields=PROFILE_FIELDS):    
        profile = self._do_api_call('users.get', { 'user_ids' :  user_id,  'fields' : fields})                    
        return profile[0]           
                            
    def get_user_profiles(self, user_ids, fields=PROFILE_FIELDS):                      
        result = []        
        for offset in xrange(0, len(user_ids) / 100 + 1):            
            start, end = offset * 100, (offset + 1) * 100 
            ids = ','.join([str(user_id) for user_id in user_ids[start:end]])        
            response = self._do_api_call('users.get', { 'user_ids' :  ids,  'fields' : fields})
            result.extend(response)
        return result
    
    def get_group_users(self, group_id, fields=PROFILE_FIELDS):
        members_count = self._do_api_call('groups.getById', { 'group_id' :  group_id,  'fields' : 'members_count'})[0]['members_count'] 
        user_ids = set()
        for offset in xrange(0, members_count / 1000 + 1):
            response = self._do_api_call('groups.getMembers', { 'group_id' :  group_id, 'offset' : offset * 1000})
            user_ids.update(response['items'])
        return list(user_ids)
    
    def get_friends(self, user_id, fields=PROFILE_FIELDS):
        response = self._do_api_call('friends.get', { 'user_id' : user_id,   'fields' : fields})                    
        return response['items']
                    
    def close():
        self.session.close()        

    def get_user_network(self, user_id, depth):          
        all_profiles = dict()
        logger.info('Getting profile for id%s' % user_id)
        all_profiles[user_id] = self.get_user_profile(user_id)
        
        queue = [(user_id, depth)]       
        while len(queue) > 0:
            head_id, head_depth = queue[0]        
            logger.info('Getting friends for id%s' % head_id)   
            friends = []
            try:
                friends = self.get_friends(head_id, fields=[])    
            except VkError as e:
                pass
            
            all_profiles[head_id]['friends'] = [int(friend['id']) for friend in friends] 
            
            if head_depth > 1:            
                for friend in friends:
                    friend_id = int(friend['id'])
                    if friend_id not in all_profiles:
                        all_profiles[friend_id] = friend
                        queue.append((friend_id, head_depth - 1))
            queue.pop(0) 
        return all_profiles 
       
def save_friends_pairs_file(file, user_network):   
    with open(file, 'w') as of:
        pairs = set()
        for k, v in user_network.iteritems():
            k = int(k)
            for fr in v['friends']:
                if fr in user_network:                    
                    if (k, fr) not in pairs:
                        of.write('%d %d\n' % (k, fr))
                        pairs.add((k, fr))
                    if (fr, k) not in pairs:
                        of.write('%d %d\n' % (fr, k))
                        pairs.add((fr, k))   
                    
def save_profiles_file(file, user_network):   
    with codecs.open(file, 'w', 'utf-8') as of:
        of.write(json.dumps(user_network, ensure_ascii=False, encoding='utf-8', indent=1)) 
                
    
if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
       
    api = VkAPI()    
    
    for user_id in open(sys.argv[1], 'r') if os.path.isfile(sys.argv[1]) else sys.argv[1:]:   
        user_id = user_id.strip()
        logger.info('Getting network for id%s' % user_id)
        
        user_network = api.get_user_network(user_id, 2)   
        
        interested_user_ids = set()
        for uid, profile in user_network.iteritems():                             
           interested_user_ids.add(uid) # profile['friends']
        user_profiles = api.get_user_profiles(list(interested_user_ids))
                
        save_profiles_file('profiles_%s.json' % user_id, user_profiles)        
        save_friends_pairs_file('egonet_%s.csv' % user_id, user_network)
        