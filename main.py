from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session


def select_one():
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


def select_two():
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
    -- 3 Знайти середній бал у групах з певного предмета.
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
    -- 4 Знайти середній бал на потоці (по всій таблиці оцінок).
    SELECT ROUND(AVG(g.grade), 2) AS average_mark
    FROM grades AS g;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2)).select_from(Grade).first()
    return result


def select_05():
    """
    -- 5 Знайти які курси читає певний викладач.
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


def select_12():
    """
    -- Оцінки студентів у певній групі з певного предмета на останньому занятті.
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
    # print(select_one())
    # print(select_two())
    # print(select_03())
    # print(select_04())
    # print(select_05())
    print(select_06())
    # print(select_12())
