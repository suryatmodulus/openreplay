import pandas as pd
from chalicelib.utils import ch_client
from datetime import datetime, timedelta


def __handle_timestep(time_step):
    assert type(time_step) == str, 'time_step must be a string {hour, day, week} or minutes in string format'
    base = "{0}"
    if time_step == 'hour':
        return f"toStartOfHour({base})", 3600
    elif time_step == 'day':
        return f"toStartOfDay({base})", 24*3600
    elif time_step == 'week':
        return f"toStartOfWeek({base})", 7*24*3600
    else:
        assert type(time_step) == int, "time_step must be {'hour', 'day', 'week'} or an integer representing the time step in minutes"
        return f"toStartOfInterval({base}, INTERVAL {time_step} minute)", int(time_step)*60


def query_requests_by_period(project_id, start_time=(datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d'),
                        end_time=datetime.now().strftime('%Y-%m-%d'), time_step=3600):
    function, steps = __handle_timestep(time_step)
    query = f"""WITH
  {function.format(f"toDateTime64('{start_time}', 0)")} as start,
  {function.format(f"toDateTime64('{end_time}', 0)")} as end
SELECT T1.hh, count(T2.session_id) as sessions, avg(T2.success) as success_rate, T2.url_host as names, groupUniqArray(T2.url_path) as sources, avg(T2.duration) as avg_duration FROM (SELECT arrayJoin(arrayMap(x -> toDateTime(x), range(toUInt32(start), toUInt32(end), {steps}))) as hh) AS T1
    LEFT JOIN (SELECT session_id, url_host, url_path, success, message, duration, {function.format('datetime')} as dtime FROM events WHERE project_id = {project_id} AND event_type = 'REQUEST') AS T2 ON T2.dtime = T1.hh GROUP BY T1.hh, T2.url_host ORDER BY T1.hh DESC;
    """
    with ch_client.ClickHouseClient() as conn:
        res = conn.execute(query=query)
    df = pd.DataFrame(res)
    del res
    first_ts, second_ts = df['hh'].unique()[:2]
    df1 = df[df['hh'] == first_ts]
    df2 = df[df['hh'] == second_ts]
    last_period_hosts = df2['names'].unique()
    this_period_hosts = df1['names'].unique()
    new_hosts = [x for x in this_period_hosts if x not in last_period_hosts]
    common_names = [x for x in this_period_hosts if x not in new_hosts]

    delta_duration = dict()
    delta_success = dict()
    for n in common_names:
        df1_tmp = df1[df1['names'] == n]
        df2_tmp = df2[df2['names'] == n]
        delta_duration[n] = df1_tmp['avg_duration'].mean() - df2_tmp['avg_duration'].mean()
        delta_success[n] = df1_tmp['success_rate'].mean() - df2_tmp['success_rate'].mean()
    # Maybe change method to nsmallest(samples, label_to_order)
    return pd.DataFrame(delta_duration.items(), columns=['host', 'increase']).sort_values(by=['increase'], ascending=False),\
           pd.DataFrame(delta_success.items(), columns=['host', 'increase']).sort_values(by=['increase'], ascending=True),\
           df1.sort_values(by=['success_rate'], ascending=True)[['names', 'success_rate']],\
           df1.sort_values(by=['avg_duration'], ascending=False)[['names', 'avg_duration']],\
           new_hosts


def query_most_errors_by_period(project_id, start_time=(datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d'),
                        end_time=datetime.now().strftime('%Y-%m-%d'), time_step=3600):
    function, steps = __handle_timestep(time_step)
    query = f"""WITH
  {function.format(f"toDateTime64('{start_time}', 0)")} as start,
  {function.format(f"toDateTime64('{end_time}', 0)")} as end
SELECT T1.hh, count(T2.session_id) as sessions, T2.name as names, groupUniqArray(T2.source) as sources FROM (SELECT arrayJoin(arrayMap(x -> toDateTime(x), range(toUInt32(start), toUInt32(end), {steps}))) as hh) AS T1
    LEFT JOIN (SELECT session_id, name, source, message, {function.format('datetime')} as dtime FROM events WHERE project_id = {project_id} AND event_type = 'ERROR') AS T2 ON T2.dtime = T1.hh GROUP BY T1.hh, T2.name ORDER BY T1.hh DESC;
    """
    with ch_client.ClickHouseClient() as conn:
        res = conn.execute(query=query)
    df = pd.DataFrame(res)
    del res
    first_ts, second_ts = df['hh'].unique()[:2]
    df1 = df[df['hh'] == first_ts]
    df2 = df[df['hh'] == second_ts]
    last_period_errors = df2['names'].unique()
    this_period_errors = df1['names'].unique()
    new_errors = [x for x in this_period_errors if x not in last_period_errors]
    common_errors = [x for x in this_period_errors if x not in new_errors]

    percentage_errors = dict()
    total = df1['sessions'].sum()
    error_increase = dict()
    for n in this_period_errors:
        percentage_errors[n] = (df1[df1['names']==n]['sessions'].sum())/total
    for n in common_errors:
        error_increase[n] = df1[df1['names']==n]['sessions'].sum() - df2[df2['names']==n]['sessions'].sum()

    return pd.DataFrame(percentage_errors.items(), columns=['error', 'percentage']),\
           pd.DataFrame(error_increase.items(), columns=['error', 'increase']),\
           new_errors, df


def query_cpu_memory_by_period(project_id, start_time=(datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d'),
                        end_time=datetime.now().strftime('%Y-%m-%d'), time_step=3600):
    function, steps = __handle_timestep(time_step)
    query = f"""WITH
  {function.format(f"toDateTime64('{start_time}', 0)")} as start,
  {function.format(f"toDateTime64('{end_time}', 0)")} as end
SELECT T1.hh, count(T2.session_id) as sessions, avg(T2.avg_cpu) as cpu_used, avg(T2.avg_used_js_heap_size) as memory_used, T2.url_host as names, groupUniqArray(T2.url_path) as sources FROM (SELECT arrayJoin(arrayMap(x -> toDateTime(x), range(toUInt32(start), toUInt32(end), {steps}))) as hh) AS T1
    LEFT JOIN (SELECT session_id, url_host, url_path, avg_used_js_heap_size, avg_cpu, {function.format('datetime')} as dtime FROM events WHERE project_id = {project_id} AND event_type = 'PERFORMANCE') AS T2 ON T2.dtime = T1.hh GROUP BY T1.hh, T2.url_host ORDER BY T1.hh DESC;"""
    with ch_client.ClickHouseClient() as conn:
        res = conn.execute(query=query)
    df = pd.DataFrame(res)
    del res
    first_ts, second_ts = df['hh'].unique()[:2]
    df1 = df[df['hh'] == first_ts]
    df2 = df[df['hh'] == second_ts]
    _tmp = df2['memory_used'].mean()
    return {'cpu_increase': df1['cpu_used'].mean() - df2['cpu_used'].mean(),
            'memory_increase': (df1['memory_used'].mean() - _tmp)/_tmp}


if __name__ == '__main__':
    # configs
    start = '2022-04-19'
    end = '2022-04-21'
    projectId = 1307
    time_step = 'hour'

    # Errors widget
    print('Errors example')
    res = query_most_errors_by_period(projectId, start_time=start, end_time=end, time_step=time_step)
    print(res)

    # Resources widgets
    print('resources example')
    res = query_cpu_memory_by_period(projectId, start_time=start, end_time=end, time_step=time_step)

    # Network widgets
    print('Network example')
    res = query_requests_by_period(projectId, start_time=start, end_time=end, time_step=time_step)
    print(res)
