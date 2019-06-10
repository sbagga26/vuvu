from rest_framework.views import APIView
import pandas as pd
from lnd.base import connectDB, error_logging, insert_into_db
from django.http import JsonResponse
import collections


class Portal_Mapping(APIView):
    def get(self, request):
        """

        :param request:
        :return:
        """
        logger = error_logging()
        try:
            engine = connectDB()
            destination_table_df = pd.read_sql_query("select * from Portals_Category_tagging", engine)
            destination_table_columns = destination_table_df.columns.get_values()
            return JsonResponse({"Output": {'columns_names': list(set(destination_table_columns))}})
        except Exception as e:
            logger.error(str(e))
            return JsonResponse({"Output": {'Error': str(e)}})

    def post(self, request):
        print(request.data)
        """

        :param request:
        :return:
        """
        # engine = connectDB()
        logger = error_logging()
        try:
            post_type = request.data['post_type']
            if post_type == 'parse_file':
                data_file = request.data['file']
                print(request.data)
                data_df = pd.read_csv(data_file)
                data_file_columns = data_df.columns.get_values()
                return JsonResponse({'uploaded_file_columns': list(set(data_file_columns))})

            elif post_type == 'final_mapping':
                data_file = request.data['file']
                mappings = request.data['final_mapping']
                data_df = pd.read_csv(data_file)
                data_df = data_df.fillna('')
                data_df = data_df.rename(mappings, axis='columns')
                if not len(data_df):
                    return JsonResponse({"Output": {"Response": "No data to insert"}})
                column_to_be_included = list()
                for mapping in mappings:
                    column_to_be_included.append(mappings[mapping])

                output = insert_into_db(df=data_df[column_to_be_included], chunk_size=100,
                                        table_name='Portals_Category_tagging')

            elif post_type == 'form_data':
                data = request.data['form_data']
                engine = connectDB()
                destination_table_df = pd.read_sql_query("select * from Portals_Category_tagging", engine)
                destination_table_columns = destination_table_df.columns.get_values()
                temp_form_data = collections.defaultdict(list)
                for column in destination_table_columns:
                    for form_data in data:
                        temp_form_data[column].append(form_data[column])
                print(temp_form_data)
                form_data_df = pd.DataFrame(temp_form_data)
                output = insert_into_db(df=form_data_df, chunk_size=100, table_name='Portals_Category_tagging')

            return JsonResponse({"Output": {"Response": output}})
        except Exception as e:
            logger.error(str(e))
            return JsonResponse({"Output": {'Error': str(e)}})
