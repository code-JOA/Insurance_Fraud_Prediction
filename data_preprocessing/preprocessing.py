import pandas as pd
import numpy as np
from sklearn_pandas import CategoricalImputer
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import RandomOverSampler


class Preprocessor:
    """This class is to clean and transform the data before training
    """
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_unwanted_spaces(self, data):
        """Method name : remove_unwanted_spaces
           Description : this method removes unwanted spaces from the df
           Output = pandas DataFrame after removing the spaces
           On failure : Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the remove_unwanted_spaces method of the Preprocessing class')
        self.data = data

        try:
            self.df_without_spaces=self.data.apply(lambda x: x.str.strip() if x.dtype=='object' else x) #drops labels specified in these columns
            self.logger_object.log(self.file_object,
              "Unwanted spaces removal Successful. Exited the reomve_unwanted_spaces_method of the Preprocessor class")
            return self.df_without_spaces
        except Exception as e:
            self.logger_object.log(self.file_object,
               "Exception occured in remove_unwanted_spaces method of the Preprocessing class.Exception message: " +str(e))
            self.logger_object.log(self.file_object,
               "unwanted space removal Unsuccessful. Exited the remove_unwanted_spaces method of the Preprocessor class"
            raise Exception()


   def remove_columns(self, data, columns):
       """Method Name : remove columns
          Description : This method reomves the given columns from the pandas DataFrame
          Output : A pandas df after removing the specified columns.
          On Failure : Raise Exception
       """
       self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
       self.data=data
       self.columns=columns
       try:
           self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
           self.logger_object.log(self.file_object,
                       "Column removal Successful. Exited the remove columns method of the Preprocessor class")
           return self.useful_data
       except Exception as e:
           self.logger_object.log(self.file_object, "Exception occured in the remove_columns method of the
                       Preprocessor class. Exception message: "+str(e))
           self.logger_object.log(self.file_object,
                       "Column reomval Unsuccessful. Exited the remove_columns method of the Preprocessor class")
           raise Exception()


    def separate_label_feature(self, data, label_column_name):
        """
        Method Name : separate_label_feature
        Description : This method separates the features and the Label columns
        Output " Returns two separate DataFrames, one containing features and the other containing Labels"
        On Failure : Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name , axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # filter the Label columns
            self.logger_object.log(self.file_object,
                "Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class")
            return self.X, self.Y
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in the separate_label_feature
                                  method of the Preprocessor class. Exception: '+ str(e))
            self.logger_object.log(self.file_object, "Label Separation Unsuccessful. Exited the separate_label_feature
                                  method of the Preprocessor class")
            raise Exception()                                            
