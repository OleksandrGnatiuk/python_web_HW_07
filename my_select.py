from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session


def select_01():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    SELECT s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 5;
    :return:
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    # order_by(Grade.grade.desc())
    return result


def select_02():
    """
    SELECT d.name, s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE d.id = 5
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 1;
    :return:
    """
    result = session.query(
        Discipline.name,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Discipline) \
        .filter(Discipline.id == 5) \
        .group_by(Student.id, Discipline.name).order_by(desc('avg_grade')).limit(1).first()
    return result


def select_03():
    """
    Знайти середній бал у групах з певного предмета.
    SELECT g2.name, d.name, ROUND(AVG(g.grade), 2)
    FROM grades g
    LEFT JOIN students AS s ON s.id  = g.student_id
    LEFT JOIN disciplines AS d ON d.id = g.discipline_id
    LEFT JOIN groups AS g2 ON g2.id = s.group_id
    WHERE d.id = 1
    GROUP BY g2.id, d.name
    ORDER BY AVG(g.grade) DESC;
    """
    result = session.query(
        Group.name,
        Discipline.name,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Discipline).join(Group) \
        .filter(Discipline.id == 1) \
        .group_by(Group.id, Discipline.name) \
        .order_by(desc(func.round(func.avg(Grade.grade), 2))).all()
    return result


def select_04():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    SELECT ROUND(AVG(g.grade), 2) AS average_mark
    FROM grades AS g;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2)).select_from(Grade).first()
    return result


def select_05():
    """
    Знайти які курси читає певний викладач.
    SELECT t.fullname, d.name
    FROM disciplines AS d
    LEFT JOIN teachers AS t ON t.id = d.teacher_id
    WHERE t.id = 2;
    """
    result = session.query(Discipline.name, Teacher.fullname) \
        .select_from(Discipline).join(Teacher) \
        .filter(Teacher.id == 2).all()
    return result


def select_06():
    """
    Знайти список студентів у певній групі.
    SELECT g.name, s.fullname
    FROM students AS s
    LEFT JOIN groups AS g ON g.id = s.group_id
    WHERE g.id = 1;
    """
    result = session.query(Group.name, Student.fullname)\
        .select_from(Student).join(Group).filter(Group.id == 1).all()
    return result


def select_07():
    """
    Знайти оцінки студентів у окремій групі з певного предмета.
    SELECT gr.name AS Group_Name, s.fullname AS student, d.name AS subject, g.grade AS mark
    FROM grades AS g
    LEFT JOIN students AS s ON s.id  = g.student_id
    JOIN disciplines AS d ON d.id = g.discipline_id
    LEFT JOIN groups AS gr ON gr.id = s.group_id
    WHERE gr.id = 3 AND d.id = 3
    ORDER BY s.fullname DESC;
    """
    result = session.query(Group.name, Discipline.name, Student.fullname, Grade.grade)\
        .select_from(Grade)\
        .join(Student).join(Group).join(Discipline)\
        .filter(Group.id == 3, Discipline.id == 3)\
        .order_by(desc(Student.fullname)).all()
    return result


def select_08():
    """
    8 Знайти середній бал, який ставить певний викладач зі своїх предметів.
    SELECT t.fullname  AS teacher , ROUND(AVG(g.grade),2) AS average_mark
    FROM disciplines d
    LEFT JOIN grades g ON g.discipline_id = d.id
    LEFT JOIN teachers t ON t.id = d.teacher_id
    WHERE t.id = 4
    group by t.fullname;
    """
    result = session.query(Teacher.fullname, func.round(func.avg(Grade.grade), 2))\
        .select_from(Discipline).join(Grade).join(Teacher)\
        .filter(Teacher.id == 4).group_by(Teacher.fullname).first()
    return result


def select_09():
    """
    Знайти список курсів, які відвідує студент.
    SELECT s.fullname AS student, d.name AS discipline
    FROM grades AS g
    LEFT JOIN students AS s ON s.id = g.student_id
    LEFT JOIN disciplines AS d ON d.id = g.discipline_id
    WHERE s.id = 9
    GROUP BY d.id, s.fullname;
    """
    result = session.query(Discipline.name, Student.fullname)\
        .select_from(Grade).join(Student).join(Discipline)\
        .filter(Student.id == 1)\
        .group_by(Discipline.id, Student.fullname).all()
    return result

def select_10():
    """
    Список курсів, які певному студенту читає певний викладач.
    SELECT d.name AS subject, s.fullname AS student, t.fullname AS teacher
    FROM grades AS g
    LEFT JOIN disciplines AS d ON d.id = g.discipline_id
    LEFT JOIN teachers AS t ON t.id = g.discipline_id
    LEFT JOIN students AS s ON s.id = g.student_id
    WHERE s.id = 1 AND t.id = 3
    GROUP BY d.id, s.fullname, t.fullname;
    """
    result = session.query(Discipline.name, Student.fullname, Teacher.fullname)\
        .select_from(Grade).join(Discipline).join(Teacher).join(Student)\
        .filter(Student.id == 2, Teacher.id == 5)\
        .group_by(Discipline.id, Student.fullname, Teacher.fullname).all()
    return result

def select_11():
    """
    Середній бал, який певний викладач ставить певному студентові.
    SELECT t.fullname AS TEACHER, s.fullname AS STUDENT, ROUND(AVG(g.grade), 2) AS AVERAGE_MARK
    FROM grades AS g
    LEFT JOIN disciplines AS d ON d.id = g.discipline_id
    LEFT JOIN teachers AS t ON t.id = d.teacher_id
    LEFT JOIN students AS s ON s.id  = g.student_id
    WHERE s.id = 4 AND t.id = 3
    GROUP BY t.fullname, s.fullname;
    """
    result = session.query(Teacher.fullname, Student.fullname, func.round(func.avg(Grade.grade),2))\
        .select_from(Grade).join(Discipline).join(Teacher).join(Student)\
        .filter(Student.id == 1, Teacher.id == 1)\
        .group_by(Teacher.fullname, Student.fullname).first()
    return result


def select_12():
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    select s.id, s.fullname, g.grade, g.date_of
    from grades g
    join students s on s.id = g.student_id
    where g.discipline_id = 3 and s.group_id = 3 and g.date_of = (
        select max(date_of)
        from grades g2
        join students s2 on s2.id = g2.student_id
        where g2.discipline_id = 3 and s2.group_id = 3
    );
    :return:
    """
    subquery = (select(func.max(Grade.date_of)).join(Student).filter(and_(
        Grade.discipline_id == 3, Student.group_id == 3
    )).scalar_subquery())

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.date_of) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.discipline_id == 3, Student.group_id == 3, Grade.date_of == subquery)).all()
    return result


if __name__ == '__main__':
    # print(select_01())
    # print(select_02())
    # print(select_03())
    # print(select_04())
    # print(select_05())
    # print(select_06())
    # print(select_07())
    print(select_08())
    # print(select_09())
    # print(select_10())
    # print(select_11())
    # print(select_12())
