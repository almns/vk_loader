```
from vkapi import VkAPI

api = VkAPI()
# or api = VkAPI(access_token)
# get user ids for club44016343
user_ids = api.get_group_users(44016343)

# get user profiles
user_profiles = api.get_user_profiles(user_ids)
user_profile = user_profiles[5]
print user_profiles[0]['last_name'], user_profiles[0]['universities']

# get friends for id1
friends = api.get_friends(1) 

# get profile for id1
durov_profile = api.get_user_profile(1)
```