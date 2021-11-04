### CS122, Winter 2021: Course search engine: search
###
### Jake Underland

from math import radians, cos, sin, asin, sqrt
import sqlite3
import os


# Use this filename for the database
DATA_DIR = os.path.dirname(__file__)
DATABASE_FILENAME = os.path.join(DATA_DIR, 'course-info.db')


def find_courses(args_from_ui):
    '''
    Takes a dictionary containing search criteria and returns courses
    that match the criteria.  The dictionary will contain some of the
    following fields:

      - dept a string
      - day a list with variable number of elements
           -> ["'MWF'", "'TR'", etc.]
      - time_start an integer in the range 0-2359
      - time_end an integer in the range 0-2359
      - walking_time an integer
      - enroll_lower an integer
      - enroll_upper an integer
      - building a string
      - terms a string: "quantum plato"]

    Returns a pair: list of attribute names in order and a list
    containing query results.

    '''

    input_options = {"dept": {"SELECT": set(["dept", "course_num", "title"]),
                          "FROM JOIN": set(["courses"]), 
                          "ON": set([]),
                          "WHERE": "courses.dept = ?"},
                 "terms": {"SELECT": set(["dept", "course_num", "title"]),
                          "FROM JOIN": set(["courses", "catalog_index"]), 
                          "ON": set(["courses.course_id = catalog_index.course_id"]),
                          "WHERE": "catalog_index.word = ?"},
                 "day": {"SELECT": set(["dept", "course_num", "section_num", "day", "time_start", "time_end"]),
                         "FROM JOIN": set(["courses", "sections", "meeting_patterns"]), 
                         "ON": set(["courses.course_id = sections.course_id", "sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id"]),
                         "WHERE": "meeting_patterns.day = ?"},
                 "time_start": {"SELECT": set(["dept", "course_num", "section_num", "day", "time_start", "time_end"]),
                         "FROM JOIN": set(["courses", "sections", "meeting_patterns"]), 
                         "ON": set(["courses.course_id = sections.course_id", "sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id"]),
                         "WHERE": "meeting_patterns.time_start >= ?"},
                 "time_end": {"SELECT": set(["dept", "course_num", "section_num", "day", "time_start", "time_end"]),
                         "FROM JOIN": set(["courses", "sections", "meeting_patterns"]), 
                         "ON": set(["courses.course_id = sections.course_id", "sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id"]),
                         "WHERE": "meeting_patterns.time_end <= ?"},
                 "walking_time":  {"SELECT": set(["dept", "course_num", "section_num", "day", "time_start", "time_end", "building", "walking_time"]),
                         "FROM JOIN": set(["courses", "sections", "meeting_patterns", "gps AS a"]), 
                         "ON": set(["courses.course_id = sections.course_id", "sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id"]),
                         "WHERE": "walking_time <= ?"},
                 "building": {"SELECT": set(["dept", "course_num", "section_num", "day", "time_start", "time_end", "building", "walking_time"]),
                         "FROM JOIN": set(["courses", "sections", "meeting_patterns", "gps AS b"]), 
                         "ON": set(["courses.course_id = sections.course_id", "sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id", "sections.building_code = b.building_code"]),
                         "WHERE": "a.building_code = ?"},
                 "enroll_lower": {"SELECT": set(["dept", "course_num", "section_num", "day", "time_start", "time_end", "enrollment"]),
                         "FROM JOIN": set(["courses", "sections", "meeting_patterns"]), 
                         "ON": set(["courses.course_id = sections.course_id", "sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id"]),
                         "WHERE": "sections.enrollment >= ?"},
                 "enroll_upper": {"SELECT": set(["dept", "course_num", "section_num", "day", "time_start", "time_end", "enrollment"]),
                         "FROM JOIN": set(["courses", "sections", "meeting_patterns"]), 
                         "ON": set(["courses.course_id = sections.course_id", "sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id"]),
                         "WHERE": "sections.enrollment <= ?"}}

    connection = sqlite3.connect(DATABASE_FILENAME) 
    c = connection.cursor()
    connection.create_function("time_between", 4, compute_time_between)

    args_copy = dict(args_from_ui)
    if args_copy.get("terms"):
        args_copy["terms"] = args_copy["terms"].split()
    
    query1 = select_func(args_copy, input_options)
    query2 = from_on_func(args_copy, input_options, True)
    query3 = from_on_func(args_copy, input_options, False)
    query4, variables1 = where_func(args_copy, input_options)
    query5, variables2 = groupby_func(args_copy)

    final_command = ( " ".join(query1 + query2 + query3 + query4 + query5), 
                     variables1 + variables2)
    search_result = c.execute(final_command[0], final_command[1])
    final_result = search_result.fetchall()
    if final_result:
        columns = get_header(c)
    else:
        columns = []
    connection.close()

    return columns, final_result


def select_func(args_from_ui, input_options):
    '''
    Creates the SELECT block
    Inputs: 
        args_from_ui: a dictionary containing all of the inputs and arguments
        input_options: a dictionary containing the input keys as keys and potential SQL outputs as values
    Outputs:
        query_str: a string acting as the SELECT statements for the SQL command
    '''
    outputs_to_fields = {"dept": "courses.", "course_num": "courses.", "title": "courses.", "section_num": "sections.", "day": "meeting_patterns.", "time_start": "meeting_patterns.", "time_end": "meeting_patterns.", "building": "b.building_code AS ", "walking_time": "time_between(a.lon, a.lat, b.lon, b.lat) AS ", "enrollment": "sections."}
    ordered_outputs = ["dept", "course_num", "section_num", "day", "time_start", "time_end", "building", "walking_time", "enrollment", "title"]
    query = set()
    query_str = []
    for input_ in args_from_ui.keys():
        query.update(input_options[input_]["SELECT"])
        
    if query:
        for select_category in ordered_outputs:
            if select_category in query:
                query_str.append(select_category)
        query_str = list(map(lambda x: "{}{}".format(outputs_to_fields[x], x), 
                                                     query_str))
        query_str = ["SELECT " + ", ".join(query_str)]
        
    return query_str


def from_on_func(args_from_ui, input_options, FROM=True):
    '''
    Creates the FROM and JOIN or ON arguments of the SQL statement. 
    
    Inputs:
      args_from_ui (dic): Dictionary with search inputs
      input_options (dic): Dictionary containing all necessary information
        per input term. 
      FROM (boolean): true if computing FROM and JOIN arguments, false if ON.
    Returns a list containing a string with the FROM part of the query.
    '''

    if FROM:
        A = "FROM JOIN"
        B = "FROM " 
        C = " JOIN "
    else:
        A = "ON"
        B = "ON "
        C = " AND "

    query = set()

    for input_ in args_from_ui.keys():
        query.update(input_options[input_][A])
    if query:
        query = [B + C.join(query)]
    else:
        query = []

    return query


def where_func(args_from_ui, input_options): 
    '''
    Creates the WHERE argument of the SQL statement. 
    
    Inputs:
      args_from_ui (dic): Dictionary with search inputs
      input_options (dic): Dictionary containing all necessary information
        per input term. 
    Returns a list containing a string with the ON part of the query.
    '''
    query = []
    tupleq = tuple()
    for input_, value in args_from_ui.items():
        if isinstance(value, list):
            subquery = []
            for instance in value:
                subquery.append(input_options[input_]["WHERE"])
                tupleq += (instance,)
            query += ["(" + " OR ".join(subquery) + ")"]
        else: 
            query.append(input_options[input_]["WHERE"])
            tupleq += (value, )
    if query:
        query = ["WHERE " +  " AND ".join(query)]

    return query, tupleq


def groupby_func(args_from_ui):
    '''
    Creates the GROUP BY and HAVING arguments of the SQL statement. 
    Only generates these when multiple terms are passed into the search
    engine. This ensures that only courses with all the terms appearing in 
    their title or description are returned by the search. 
    
    Inputs:
      args_from_ui (dic): Dictionary with search inputs
      input_options (dic): Dictionary containing all necessary information
        per input term. 
    Returns a tuple with a list containing a string with the FROM part of 
      the query and a tuple containing the variable for the HAVING COUNT(*)
      statement. 
    '''
    query = []
    tupleq = tuple()
    if args_from_ui.get("terms"):
        num_terms = len(args_from_ui["terms"])
        if num_terms > 1:
            query = ["GROUP BY courses.course_id HAVING COUNT(*) >= ?"]
            tupleq = (num_terms,)
    
    return query, tupleq



########### do not change this code #############

def compute_time_between(lon1, lat1, lon2, lat2):
    '''
    Converts the output of the haversine formula to walking time in minutes
    '''
    meters = haversine(lon1, lat1, lon2, lat2)

    # adjusted downwards to account for manhattan distance
    walk_speed_m_per_sec = 1.1
    mins = meters / (walk_speed_m_per_sec * 60)

    return mins


def haversine(lon1, lat1, lon2, lat2):
    '''
    Calculate the circle distance between two points
    on the earth (specified in decimal degrees)
    '''
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))

    # 6367 km is the radius of the Earth
    km = 6367 * c
    m = km * 1000
    return m


def get_header(cursor):
    '''
    Given a cursor object, returns the appropriate header (column names)
    '''
    desc = cursor.description
    header = ()

    for i in desc:
        header = header + (clean_header(i[0]),)

    return list(header)


def clean_header(s):
    '''
    Removes table name from header
    '''
    for i, _ in enumerate(s):
        if s[i] == ".":
            s = s[i + 1:]
            break

    return s


########### some sample inputs #################

EXAMPLE_0 = {"time_start": 930,
             "time_end": 1500,
             "day": ["MWF"]}

EXAMPLE_1 = {"dept": "CMSC",
             "day": ["MWF", "TR"],
             "time_start": 1030,
             "time_end": 1500,
             "enroll_lower": 20,
             "terms": "computer science"}