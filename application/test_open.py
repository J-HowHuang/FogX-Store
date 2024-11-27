import pickle
import pandas
import os

with open('with_step.pickle', 'rb') as file:
    my_object = pickle.load(file)

print(my_object)
print(my_object.shape)
print(type(my_object))