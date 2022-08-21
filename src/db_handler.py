import db_connections as db
import json


def read_from_db(category, possible_vendors):
    query = """
        --assigned and waiting for response
        with wait_for_respnse as (
            select project_vendor_id,
                   last_update assigned_date,
                   1 as awaiting_pro_response
            from
                (select project_vendor_id,
                        last_update,
                        row_number() over (partition by project_vendor_id order by last_update asc ) as flag
                 from "Project_Vendor_History"
                 where project_vendor_status=1) as apr
            where flag =1
        ),
        
        --after pro response
             in_progress as (
                 select project_vendor_id,
                        last_update response_date,
                        1 as          in_progress
                 from (select project_vendor_id,
                              last_update,
                              row_number() over (partition by project_vendor_id order by last_update asc ) as flag
                       from "Project_Vendor_History"
                       where project_vendor_status = 10) as ip
                 where flag = 1
             ),
        
             project_vendor_response_time as (
                 select i.project_vendor_id project_vendor_id, w.assigned_date assigned_date, i.response_date response_date, Round((extract(epoch from (i.response_date-w.assigned_date))/60)) response_time
                 from wait_for_respnse w
                          inner join in_progress i on i.project_vendor_id=w.project_vendor_id
             ),
        
            projects_canceled_by_vendor as (
                select pv.vendor_id, pv.category_id, count(pv.project_id) as rejected_proj_counter from "Project_Vendor" as pv
                inner join "Project_Vendor_Status" as pvs
                    on pvs.project_vendor_status_id = pv.project_vendor_status
                where pvs.status = 'Rejected by Pro'
        --         and DATE_PART('day', now()-  pv.last_update) < 30
                group by pv.vendor_id, pv.category_id
            ),
        
             projects_active_by_vendor as (
                 select pv.vendor_id, pv.category_id, count(pv.project_id) as active_proj_counter from "Project_Vendor" as pv
                 inner join "Project_Vendor_Status" as pvs
                            on pvs.project_vendor_status_id = pv.project_vendor_status
                 where pvs.status in ('Waiting for Pro Response', 'In Progress', 'Pending Completion')
                    and pv.project_vendor_close = 0
        --             and DATE_PART('day', now()-  pv.last_update) < 30
                 group by pv.vendor_id, pv.category_id
             ),
        
            projects_reassigned_per_vendor as (
                SELECT pv.vendor_id, pv.category_id, count(pv.project_id) as reassigned_proj FROM PUBLIC."Project_Vendor" AS pv
                WHERE pv.state = 11
                AND pv.Project_Vendor_Status IN (1, 16, 19, 20)
                AND pv.reject_reason IN (' Rejecting project', 'null', 'no response', 'not assisting', 'not needed', 'another vendor will take care of this',
                'unresponsive')
                group by pv.vendor_id, pv.category_id
            )
        
        
        -- select project_vendor_id, assigned_date, response_date, Round((extract(epoch from (response_date-assigned_date))/60)) response_time
        -- from project_vendor_response_time;
        
        select distinct v.vendor_id, v.full_name, v.email, categ.category_id, categ.name as category_name, v.date_added, count(pv.project_id) as num_of_projects, avg(pvr.weighted_rating) avg_rating,
                        avg(resp_time.response_time) avg_response_time, pvc.rejected_proj_counter as number_of_rejected, pva.active_proj_counter as number_of_active, reassigned.reassigned_proj
        from "Vendors" as v
            inner join "Vendor_Category" as vc
                on vc.vendor_id = v.vendor_id
            inner join "Categories" as categ
                on categ.category_id = vc.category_id
            left outer join "Project_Vendor" as pv
                on pv.vendor_id = v.vendor_id
                and pv.category_id = vc.category_id
            left outer join "projects_canceled_by_vendor" as pvc
                on pvc.vendor_id = v.vendor_id
                and pvc.category_id = vc.category_id
            left outer join "projects_active_by_vendor" as pva
                on pva.vendor_id = v.vendor_id
                and pva.category_id = vc.category_id
            left outer join "projects_reassigned_per_vendor" as reassigned
                on reassigned.vendor_id  = v.vendor_id
                and reassigned.category_id = vc.category_id
            left outer join "project_vendor_response_time" as resp_time
                on resp_time.project_vendor_id = pv.project_vendor_id
            left outer join "Project_Vendor_Rate" as pvr
                on pvr.project_vendor_id = pv.project_vendor_id
        
        where v.vendor_active_status = 1
        and categ.name = '""" + category + """'
        and v.vendor_id in ('""" + possible_vendors + """')
        group by v.vendor_id, v.full_name, v.email, v.date_added, pvc.rejected_proj_counter, pva.active_proj_counter, categ.category_id, categ.name, reassigned.reassigned_proj
        order by v.vendor_id;
            ;"""

    df = db.importDataFromPG(query)
    return df
