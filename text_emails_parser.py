#!/usr/bin/python

author = '__michael__'

'''
Purpose: parse one or several emails (converted as txt files)

'''
import os
import pandas as pd
from dateutil import parser

class TextEmailParser():
    '''
    Operations:
    - reading text email file,
    - extracting key-infos,
    - xls updating
    start with test function
    move core part to actual function

    Intermediate data structure:
    - dictionary {num_cmd: str,
                  distributeur: str,
                  client: str,
                  num_compte: str,
                  detail_cmd: list dict(s) [{description: str long,
                                             produit: str,
                                             num_contrat: str,
                                             date_debut: date,
                                             date_fin: date,
                                             qté: int}]
                }

    parsing sur Durée de la souscription

    Numéro de commande (PO)    : C1920106
    Distributeur               : Tech Data France, SAS


    Client final                :Interforum
    Numéro de compte du client final:525622
    Numéro de commande du client final:C1920106

    Détail de la commande:

    ********
    Ligne 10
    Description  : Red Hat Enterprise Linux for Virtual Datacenters with Smart Management, Standard
    Produit      : RH00007
    Numéro de contrat: 11976463
    Durée de la souscription: du 01-AUG-2019 au 31-DEC-2021
    Quantité     : 1
    ********
    Ligne 20
    Description  : Red Hat Enterprise Linux Server with Smart Management, Standard (Physical or Virtual Nodes)
    Produit      : RH00009
    Numéro de contrat: 11976463
    Durée de la souscription: du 01-AUG-2019 au 31-DEC-2021
    Quantité     : 6
    ********

    ALGO:
    - get infos: num_cmd, distributeur, client, numero_compte_client
    - get ligne block infos:
      - Description
      - Produit
      - num_contrat
      - date_debut
      - date_fin
      - qte
      --> save in xls for this line
    '''

    def __init__(self):
        self.input_dir = '/home/michael/Documents/ATOUT_LIBRE/PYTHON_EMAILS_PARSING/EMAILS/'
        self.input_file = '/home/michael/Documents/ATOUT_LIBRE/PYTHON_EMAILS_PARSING/email_to_parse.txt'

        self.above_infos_labels = [
                                  'Numéro de commande (PO)',
                                  'Distributeur',
                                  'Client final',
                                  'Numéro de compte du client final',
                                  'Numéro de commande du client final',
                                 ]
        self.keyinfo_labels = ['numero_cmd',
                               'distributeur',
                               'client_final',
                               'numero_compte_client',
                               'numero_contrat',
                               'date_debut',
                               'date_fin',]

        self.block_ligne_labels = [
                'Description',
                'Produit',
                'Numéro de contrat',
                'Durée de la souscription',
                'Quantité'
                ]

        self.dict_ligne_record = {}
        self.df_columns = ['Numéro de commande (PO)',
                        'Distributeur',
                        'Client final',
                        'Numéro de compte du client final',
                        'Numéro de commande du client final',
                        'Description',
                        'Produit',
                        'Numéro de contrat',
                        'Date de début',
                        'Date de fin']
        self.dataframe = pd.DataFrame(columns=self.above_infos_labels)
        self.count_lignes = 0

        self.file_content_as_list = []


    def duree_souscription_parser(self, input_period):
        '''
        Input:
        - 'Durée de la souscription: du 01-AUG-2019 au 31-DEC-2021
        - du 01-AUG-2019 au 31-DEC-2021
        Output:
        - {date_debut: date(01/08/2019),
           date_fin: date(31/12/2021)}
        Assumptions:
        -
        '''
        import datetime

        try:
            # 'Durée de la souscription: du 01-AUG-2019 au 31-DEC-2021':
            # use a function to extract from Ligne block
            # input_period = 'Durée de la souscription: du 01-AUG-2019 au 31-DEC-2021'
            print('duree_souscription_parser input_period %s'%input_period)
            extracted_date_debut_str = \
                input_period.split('du')[1].split('au')[0][1:-1]
            extracted_date_final_str = \
                input_period.split('du')[1].split('au')[1][1:]

            date_debut = parser.parse(extracted_date_debut_str)
            date_final = parser.parse(extracted_date_final_str)

            dict_extracted_dates = {'date_debut': [date_debut],
                                    'date_final': [date_final]}

            df_tmp = pd.DataFrame(data = dict_extracted_dates)

            #
            self.dataframe = pd.concat([self.dataframe, df_tmp], axis=1)
            # self.dataframe.drop(['Durée de la souscription'], axis=1, inplace=True)
        except Exception as e:
            print('Error in duree_souscription_parser %s'%e.__str__())
            raise


    def make_date_debut(self):
        ''' Duree --> date date in datetime format '''
        extracted_date_debut_str = \
            input_period.split('du')[1].split('au')[0][1:-1]
        date_debut = parser.parse(extracted_date_debut_str)
        return date_debut


    def transform_dates(self):
        ''' '''
        self.dataframe['date_debut'] = \
            self.dataframe['Durée de la souscription']\
                .map(lambda x: parser\
                    .parse(x.split('du')[1].split('au')[0][1:-1]))
        self.dataframe['date_final'] = \
            self.dataframe['Durée de la souscription']\
                .map(lambda x: parser\
                    .parse(x.split('du')[1].split('au')[1][1:]))


    def save_record_into_xls(self):
        '''
        save a Ligne info into existing xls
        '''
        try:
            self.dataframe.to_excel('output_test_08dec19.xls')
        except Exception as e:
            print('Error in save_record_into_xls %s'%e.__str__())
            raise


    def find_indices(self, aList, fCondition):
        ''' '''
        # find_indices(self.above_infos_labels, lambda e: e in aKey)
        # gerer le cas ou pas d'indice
        try:
            return [idx for idx, (k,elem) in enumerate(aList) if fCondition(k)]
        except Exception as e:
            print('Error in find_indices %s'%e.__str__())
            raise


    def read_all_lines(self):
        try:
            with open(self.input_file) as f:
                content = f.readlines()
                content = [x.strip() for x in content]
            self.file_content_as_list = content
        except Exception as e:
            print('Error in read_all_lines %s'%e.__str__())
            raise


    def fill_dataframe_one_record(self, ligne_iterator):
        self.read_all_lines()
        #dataframe = pd.DataFrame(columns = self.df_columns)
        dict_tmp = {}

        try:
            # 1- df above_infos
            for key in self.above_infos_labels:
                matching_elements = [x.strip().split(':')[1].strip() for \
                    x in self.file_content_as_list if \
                    key == x.strip().split(':')[0].strip()]
                print('matching_elements %s'%matching_elements)
                if matching_elements:
                    dict_tmp[key] = matching_elements
            print('idct_tmp %s'%dict_tmp)
            dataframeA = pd.DataFrame(data = dict_tmp)
            print(dataframeA.columns)

            # 2- df specific ligne block
            # get list index of Ligne + str((iterator+1)*10)
            dict_tmp = {}

            index_Ligne_specific = \
                [idx for idx in range(len(self.file_content_as_list)) if \
                    self.file_content_as_list[idx] == \
                    'Ligne '+str((ligne_iterator)*10)][0]

            # 3- read next 5 elements
            for idx_label, key in enumerate(self.block_ligne_labels):
                dict_tmp[key] =\
                    [self.file_content_as_list[index_Ligne_specific+1+idx_label]\
                    .strip().split(':')[1].strip()]
            dataframeB = pd.DataFrame(data = dict_tmp)
            print(dataframeB.columns)
            print('**** dataframeB ***** %s'%dataframeB)

            # 4- concatenate 2 dataframes
            df = pd.concat([dataframeA, dataframeB], axis=1)

            # 5- update self.dataframe
            if self.dataframe.empty:
                self.dataframe = df
            else:
                print('ligne_iterator %s'%ligne_iterator)
                print('df %s'%df)
                self.dataframe = self.dataframe.append(df)
            print('self.dataframe %s'%self.dataframe)

            # 6- replace Durée de souscription column by 2 cols Date début et Date fin
            print('duree_souscription_parser: %s'%\
                type(self.dataframe['Durée de la souscription'].values))
            # self.duree_souscription_parser(
            #    self.dataframe['Durée de la souscription'].values[0])
        except Exception as e:
            print('Error in fill_dataframe_one_record %s'%e.__str__())
            raise


    def get_number_ligne_blocks(self):
        '''
        count number of Ligne blocks
        '''
        self.read_all_lines()
        number_blocks = \
            len([x for x in self.file_content_as_list if 'Ligne ' in x])
        print('***** number_blocks ****** %s'%str(number_blocks))
        return number_blocks


    def text_email_parser(self, atxtFile):
        '''
        group all operations on an email
        - read file,
        - parse infos,
        - build dataframe,
        - save into xls

        reading a text mail file and extracting key infos
        '''
        try:
            # -1 update self.input_file
            self.input_file = os.path.join(self.input_dir, atxtFile)

            # 2- get number of Ligne blocks
            number_blocks = self.get_number_ligne_blocks()

            # 3- build dataframe: get the propoer number of Ligne!!
            for num_Ligne in range(1, number_blocks+1):
                emailparser.fill_dataframe_one_record(num_Ligne)

        except Exception as e:
            print('Error in text_email_parser %s'%e.__str__())
            raise


    def loop_operations_over_emails(self):
        ''' '''
        try:
            # 1- build overall dataframe
            for filename in os.listdir(self.input_dir):
                if filename.endswith(".txt"):
                    self.text_email_parser(filename)
                else:
                    continue

            # 2- transform dates
            emailparser.transform_dates()

            # 3- save dataframe to xls
            emailparser.save_record_into_xls()
            print('dataframe %s'%emailparser.dataframe)
            print('dataframe cols %s'%emailparser.dataframe.columns)
            print('dataframe.shape %s'%str(emailparser.dataframe.shape))

            # 4- remove first useless column
            self.dataframe.drop(self.dataframe.columns[[0]], axis=1, inplace=True)
        except Exception as e:
            print('Error in loop_operations_over_emails %s'%e.__str__())
            raise

# main section
emailparser = TextEmailParser()
emailparser.loop_operations_over_emails()
