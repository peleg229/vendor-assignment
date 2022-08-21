import json

import pandas as pd
import configuration as config
import db_handler

INPUT_PATH = "C:\\Users\\noam\\PycharmProjects\\vendor-chooser\\vendors.csv"

## TODO: retrieve the vendors by specific category from DB
def _retrieve_vendors(category, possible_vendors):
    orig_df = db_handler.read_from_db(category, "', '".join(possible_vendors))
    ## v.vendor_id, v.full_name, categ.category_id, categ.name, v.date_added, count(pv.project_id) as num_of_projects, avg(pvr.weighted_rating) AvgRating,
    ## avg(resp_time.response_time) AvgResponseTime, pvc.rejected_proj_counter as number_of_rejected, pva.active_proj_counter as number_of_active

    # orig_df = pd.read_csv(INPUT_PATH, encoding='ISO-8859-1', skipinitialspace=True, error_bad_lines=False)
    orig_df.columns = [config.VENDOR_ID, config.VENDOR_NAME, config.EMAIL,config.CATEGORY_ID, config.CATEGORY_NAME, config.DATE_ADDED,
                       config.TOTAL_NUMBER_OF_PROJECTS, config.AVG_RATING, config.AVG_RESPONSE_TIME,
                       config.NUMBER_OF_REJECTED, config.NUMBER_OF_ACTIVE, config.NUMBER_OF_REASSIGNED]
    return orig_df


def _retrieve_vendors_from_csv():
    ## v.vendor_id, v.full_name, categ.category_id, categ.name, v.date_added, count(pv.project_id) as num_of_projects, avg(pvr.weighted_rating) AvgRating,
    ## avg(resp_time.response_time) AvgResponseTime, pvc.rejected_proj_counter as number_of_rejected, pva.active_proj_counter as number_of_active

    orig_df = pd.read_csv(INPUT_PATH, encoding='ISO-8859-1', skipinitialspace=True, error_bad_lines=False)
    orig_df.columns = [config.VENDOR_ID, config.VENDOR_NAME, config.CATEGORY_ID, config.CATEGORY_NAME, config.DATE_ADDED,
                       config.TOTAL_NUMBER_OF_PROJECTS, config.AVG_RATING, config.AVG_RESPONSE_TIME,
                       config.NUMBER_OF_REJECTED, config.NUMBER_OF_ACTIVE, config.NUMBER_OF_REASSIGNED]
    return orig_df


def is_first_assignment(last_assigned_vendor_id, vendor_df_sorted):
    return vendor_df_sorted.loc[vendor_df_sorted[config.VENDOR_ID] == last_assigned_vendor_id][config.TOTAL_NUMBER_OF_PROJECTS].iloc[0] == 0


def _find_new_vendor(df_vendors:pd.DataFrame, last_assigned_vendor_id):
    vendor_df_sorted = df_vendors.sort_values(config.TOTAL_NUMBER_OF_PROJECTS).reset_index()

    ## run over the first vendors which are considered as new and check for the first one which hasn't been assigned yet
    if last_assigned_vendor_id == config.NO_VENDOR_ID and \
            is_first_assignment(vendor_df_sorted.loc[0][config.VENDOR_ID], vendor_df_sorted):
        return vendor_df_sorted.loc[0]

    ## find a raw right after the last_assigned_vendor_id ?
    idx = vendor_df_sorted.index.get_loc(vendor_df_sorted[vendor_df_sorted[config.VENDOR_ID] == last_assigned_vendor_id].index[0])
    if idx >= 0 and idx + 1 < len(vendor_df_sorted):
        vib = vendor_df_sorted.iloc[idx+1]
        if vib[config.TOTAL_NUMBER_OF_PROJECTS] == 0:
            return vib
        else:
            return None

    raise Exception("last_assigned_vendor_id " + str(last_assigned_vendor_id) + " was not found")


def _calc_vendor_value(row):
    active_projects_ratio = row[config.NUMBER_OF_ACTIVE] / row[config.TOTAL_NUMBER_OF_PROJECTS]
    reassigned_projects_ratio = row[config.NUMBER_OF_REASSIGNED] / row[config.TOTAL_NUMBER_OF_PROJECTS]
    distance = config.w_avg_rating * row[config.FIXED_AVG_RATING] + config.w_active * active_projects_ratio + \
               config.w_rejected * row[config.FIXED_REJECTED] + config.w_avg_response_time * row[config.FIXED_AVG_RESPONSE_TIME] + \
               config.w_reassigned * reassigned_projects_ratio
    return distance

    ## euclidean matric
    # vendor = (config.w_avg_rating * row[config.FIXED_AVG_RATING], config.w_active * active_projects_ratio,
    #           config.w_rejected * row[config.FIXED_REJECTED], config.w_avg_response_time * row[config.FIXED_AVG_RESPONSE_TIME],
    #           config.w_reassigned * row[config.FIXED_REASSIGNED])
    # zero = (0, 0, 0, 0, 0)
    # return distance.euclidean(vendor, zero)

## convert values according to their scale (if we have it on negative - change it according to it completeness
def _convert_values(df_old_vendors):
    df_old_vendors[config.AVG_RESPONSE_TIME] = df_old_vendors.apply(
        lambda row: df_old_vendors[[config.AVG_RESPONSE_TIME]].max() - row[config.AVG_RESPONSE_TIME], axis=1)
    # df_old_vendors[config.NUMBER_OF_REJECTED] = df_old_vendors.apply(
    #     lambda row: df_old_vendors[[config.NUMBER_OF_REJECTED]].max() - row[config.NUMBER_OF_REJECTED], axis=1)
    # df_old_vendors[config.NUMBER_OF_REASSIGNED] = df_old_vendors.apply(
    #     lambda row: df_old_vendors[[config.NUMBER_OF_REASSIGNED]].max() - row[config.NUMBER_OF_REASSIGNED], axis=1)
    return df_old_vendors


def _get_vendor_by_queue(df_old_vendors, last_assigned_vendor_id):
    idx = df_old_vendors.index.get_loc(
        df_old_vendors[df_old_vendors[config.VENDOR_ID] == last_assigned_vendor_id].index[0])
    if idx >= 0 and idx + 1 < len(df_old_vendors):
        vib = df_old_vendors.iloc[idx + 1]
        return vib

    raise Exception("no winning vendor was found")

def _get_vendor_by_calc(df_vendors:pd.DataFrame, last_assigned_vendor_id):
    ## filter out the new vendors - not relevant anymore
    df_old_vendors =  df_vendors.loc[df_vendors[config.TOTAL_NUMBER_OF_PROJECTS] > 0]
    df_old_vendors = _convert_values(df_old_vendors)

    ## avg_response_time
    df_old_vendors[config.FIXED_AVG_RESPONSE_TIME] = df_old_vendors.apply(
        lambda row: (row[config.AVG_RESPONSE_TIME] - df_old_vendors[[config.AVG_RESPONSE_TIME]].min()) /
                    (df_old_vendors[[config.AVG_RESPONSE_TIME]].max() - df_old_vendors[[config.AVG_RESPONSE_TIME]].min()), axis=1)
    ## avg_rating
    df_old_vendors[config.FIXED_AVG_RATING] = df_old_vendors.apply(
        lambda row: (row[config.AVG_RATING] - df_old_vendors[[config.AVG_RATING]].min()) /
                    (df_old_vendors[[config.AVG_RATING]].max() - df_old_vendors[[config.AVG_RATING]].min()), axis=1)
    ## rejected_projects
    df_old_vendors[config.FIXED_REJECTED] = df_old_vendors.apply(
        lambda row: (row[config.NUMBER_OF_REJECTED] - df_old_vendors[[config.NUMBER_OF_REJECTED]].min()) /
                    (df_old_vendors[[config.NUMBER_OF_REJECTED]].max() - df_old_vendors[[config.NUMBER_OF_REJECTED]].min()), axis=1)
    # ## reassigned projects
    # df_old_vendors[config.FIXED_REASSIGNED] = df_old_vendors.apply(
    #     lambda row: (row[config.NUMBER_OF_REASSIGNED] - df_old_vendors[[config.NUMBER_OF_REASSIGNED]].min()) /
    #                 (df_old_vendors[[config.NUMBER_OF_REASSIGNED]].max() - df_old_vendors[[config.NUMBER_OF_REASSIGNED]].min()), axis=1)

    df_old_vendors[config.VALUE] = df_old_vendors.apply(_calc_vendor_value, axis=1)
    df_old_vendors = df_old_vendors.sort_values(config.VALUE, ascending=False).reset_index()

    ## if the vendor is reassigned
    if last_assigned_vendor_id == config.NO_VENDOR_ID:
        return df_old_vendors.loc[0]
    else:
        winning_vendor =_get_vendor_by_queue(df_old_vendors, last_assigned_vendor_id)
        return winning_vendor


### main flow ###
def run_flow(jsonreq):
                da = dict(json.loads(jsonreq))
                category = da['category']
                if 'last_assigned_vendor_id' in da:
                    last_assigned_vendor_id = da['last_assigned_vendor_id']
                else:
                    last_assigned_vendor_id = config.NO_VENDOR_ID
                possible_vendors = da['possible_vendors']
                # df_vendors = _retrieve_vendors_from_csv()
                df_vendors = _retrieve_vendors(category, possible_vendors)
                if len(df_vendors) == 0:
                    print("no vendors found")
                    return None
                vendor = _find_new_vendor(df_vendors, last_assigned_vendor_id)
                if vendor is None:
                    vendor = _get_vendor_by_calc(df_vendors, last_assigned_vendor_id)

                print('and the winning vendor is {}'.format(vendor))
                return json.dumps(str(vendor))

if __name__ == '__main__':
    # run_flow()
    # run_flow(last_assigned_vendor_id=1)
    jst = {'category': 'Appliance Installer / Repair', 'location_id': 'E17467CD-780C-4067-AC07-AB35B48EC22E', 'incident_id': '915844AB-6B46-4D4B-A359-F6B7CA84A8F8', 'possible_vendors': ('5392B43C-0741-4958-B144-A677AA1F907F', '7366BD64-5679-4C36-9133-AFDCA934DCD1', '2BB23931-652A-44AD-AD18-B4B09FE4916E', 'C3E72AA4-1D2C-426A-B0D1-F3AD78DDEB21', '19BA08DE-7509-4AA0-AB53-37447CA91D98')}
    jsr = json.dumps(jst)
    run_flow(jsr)
