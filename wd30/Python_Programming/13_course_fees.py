parent_course_name='datascience'
child_course_name='deep learning'
if parent_course_name == 'Bigdata':
    if child_course_name == 'Spark':
        print("only spark then fees is 15000")
    else:
        print("Bigdata then fees is 25000")
elif parent_course_name == 'datascience':
    if child_course_name == 'machinelearning':
        print("datascience with machinelearning then 25000")
    elif child_course_name == 'deep learning':
        print("datascience with deep learning then 45000")
    else:
        print("Both machinelearning and deep learning so 70000")






