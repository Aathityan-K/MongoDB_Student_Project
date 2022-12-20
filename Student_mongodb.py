#!/usr/bin/env python
# coding: utf-8

# # Student Database (MongoDB)

# In[7]:


import pymongo


# In[8]:


client = pymongo.MongoClient("mongodb://localhost:27017/")


# In[9]:


db = client.get_database('Project')


# In[10]:


records = db.Students


# In[19]:


#to check connection
x = records.find_one(32)
print(x)


# ## 1)      Find the student name who scored maximum scores in all (exam, quiz and homework)?

# In[25]:


#$unwind operator is used to deconstruct the documents in MongoDB.
#If the operand resolves to a non-empty array, $first returns the first element in the array.
#a group specification must include an _id, so "_id" should be added.

for i in records.aggregate([{"$unwind":"$scores"},{"$group":{"_id":"$_id", "name":{"$first":"$name"}, "max":{"$sum":"$scores.score"},}},{"$sort":{"max":-1}},{"$limit":1}]):
    print(i)


# ## 2)      Find students who scored below average in the exam and pass mark is 40%?

# In[26]:


#$match operator filters the documents to pass only those documents that match the specified condition (s) to the next pipeline stage.
for x in records.aggregate([{"$unwind":"$scores"},{"$match":{"scores.type":"exam", "scores.score":{"$gt":40, "$lt":60}}}]):
    print(x)


# ## 3)      Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories.

# In[30]:


for y in records.aggregate([{"$set":{"scores":{"$arrayToObject":[{"$map": 
           {"input": "$scores",
            "as": "s",
            "in": {"k": "$$s.type", "v": "$$s.score"}}}]}}},{"$project":
  {
     "_id":1,
     "name":1,
     "result":{"$cond":{"if": {"$and" : [{"$gte": ["$scores.exam", 40]}, {"$gte": ["$scores.quiz", 40]}, {"$gte": [ "$scores.homework", 40]}]},
                    "then" :"pass",
                    "else":"fail"}}}}]):
    print(y)


# ## 4)       Find the total and average of the exam, quiz and homework and store them in a separate collection

# In[32]:


average = db.student_avg_total


# In[35]:


data = []
for j in records.aggregate([{"$unwind":"$scores"},{"$group":{"_id":"$_id","name":{"$first":"$name"},"total":{"$sum":"$scores.score"},"average":{"$avg":"$scores.score"}}},{"$sort":{"_id":1}}]):
    data.append(j)
print(data)
average.insert_many(data)


# ## 5)      Create a new collection which consists of students who scored below average and above 40% in all the categories.

# In[38]:


below_average = db.belowavg


# In[39]:


below_avg = []
for n in records.aggregate([{"$match":{"$expr":{"$and":[{"$gt":[{"$min":"$scores.score"},40]},{"$lt":[{"$max":"$scores.score"},70]}]}}}]):
    below_avg.append(n)
print(below_avg)

below_average.insert_many(below_avg)


# ## 6)      Create a new collection which consists of students who scored below the fail mark in all the categories

# In[40]:


fail = db.fail


# In[42]:


failure = []
for i in records.aggregate([{"$match":{"$expr":{"$lt":[{"$max":"$scores.score"},40]}}}]):
    failure.append(i)
print(failure)

fail.insert_many(failure)


# ## 7)      Create a new collection which consists of students who scored above pass mark in all the categories.

# In[43]:


pass_mark = db.pass_mark


# In[44]:


passs = []
for i in records.aggregate([{"$match": {"$expr": {"$gt": [{"$min":"$scores.score"}, 40]}}}]):
    passs.append(i)
print(passs)

pass_mark.insert_many(passs)


# In[ ]:




