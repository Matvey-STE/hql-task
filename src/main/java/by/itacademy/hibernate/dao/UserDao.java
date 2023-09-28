package by.itacademy.hibernate.dao;


import by.itacademy.hibernate.entity.*;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQuery;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hibernate.Session;

import java.time.LocalDate;
import java.util.List;

import static by.itacademy.hibernate.entity.QCompany.*;
import static by.itacademy.hibernate.entity.QPayment.*;
import static by.itacademy.hibernate.entity.QUser.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserDao {

    private static final UserDao INSTANCE = new UserDao();

    /**
     * Возвращает всех сотрудников
     */
    public List<User> findAll(Session session) {
        return new JPAQuery<User>(session).select(user).from(user).fetch();
    }

    /**
     * Возвращает всех сотрудников с указанным именем
     */
    public List<User> findAllByFirstName(Session session, String firstName) {
        return session.createQuery("""
                        select u
                        from User u 
                        where u.personalInfo.firstname = :firstName
                        """, User.class)
                .setParameter("firstName", firstName)
                .list();
    }

    /**
     * Возвращает первые {limit} сотрудников, упорядоченных по дате рождения (в порядке возрастания)
     */
    public List<User> findLimitedUsersOrderedByBirthday(Session session, int limit) {
        return session.createQuery("""
                        select u
                        from User u 
                        join  u.personalInfo pi
                        order by pi.birthDate
                        """, User.class)
                .setMaxResults(limit)
                .list();
    }

    /**
     * Возвращает всех сотрудников компании с указанным названием
     */
    public List<User> findAllByCompanyName(Session session, String companyName) {
        return session.createQuery("""
                        select u
                        from User u 
                        join u.company c
                        where c.name = :company
                        """, User.class)
                .setParameter("company", companyName)
                .list();
    }

    /**
     * Возвращает все выплаты, полученные сотрудниками компании с указанными именем,
     * упорядоченные по имени сотрудника, а затем по размеру выплаты
     */
    public List<Payment> findAllPaymentsByCompanyName(Session session, String companyName) {

        return session.createQuery("""
                        select p
                        from Payment p
                        join p.receiver u
                        join u.company c
                        where c.name = :company
                        """, Payment.class)
                .setParameter("company", companyName)
                .list();
    }

    /**
     * Возвращает среднюю зарплату сотрудника с указанными именем и фамилией
     */
    public Double findAveragePaymentAmountByFirstAndLastNames(Session session, String firstName, String lastName) {
        return (Double) session.createQuery("""
                        select avg (p.amount)
                        from Payment p
                        join p.receiver u
                        join u.personalInfo i
                        where i.firstname = :firstName
                        and i.lastname = :lastName
                        """)
                .setParameter("firstName", firstName)
                .setParameter("lastName", lastName)
                .getSingleResult();
    }

    /**
     * Возвращает для каждой компании: название, среднюю зарплату всех её сотрудников. Компании упорядочены по названию.
     */
    public List<Object[]> findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(Session session) {
        return session.createQuery("""
                        select c.name, avg(p.amount)
                        from Company c
                        join c.users u
                        join u.payments p
                        group by c.name
                        order by c.name
                        """)
                .list();
    }

    /**
     * Возвращает список: сотрудник (объект User), средний размер выплат, но только для тех сотрудников, чей средний размер выплат
     * больше среднего размера выплат всех сотрудников
     * Упорядочить по имени сотрудника
     */
    public List<Object[]> isItPossible(Session session) {
        return session.createQuery("""
                        select u, avg (p.amount)
                        from User u
                        join u.payments p
                        join u.personalInfo pi
                        group by u
                        having avg (p.amount) > (select avg (p2.amount) from User u2 join u2.payments p2)
                        order by pi.firstname
                        """)
                .list();
    }

    /**
     * Возвращает всех сотрудников с указанным фамилиями
     */
    public List<User> findAllByLastName(Session session, String lastName) {
        return new JPAQuery<User>(session).select(user).from(user)
                .where(user.personalInfo.lastname.eq(lastName)).fetch();
    }

    /**
     * Возвращает всех сотрудников с указанной датой рождения
     */
    public List<User> findAllByUserByDateOfBirth(Session session, LocalDate dateOfBirth) {
        return new JPAQuery<User>(session).select(user).from(user)
                .where(user.personalInfo.birthDate.eq(new Birthday(dateOfBirth))).fetch();
    }

    /**
     * Возвращает Фамилию Имя сотрудника, компанию и среднюю зарплату
     * отсортированной по зарплате
     */
    public List<Tuple> findAllWithFirstNameLastNameCompanyAndAvgSalary(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(user.personalInfo.lastname,
                        user.personalInfo.firstname,
                        company.name,
                        payment.amount.avg(),
                        user.id)
                .from(user)
                .join(user.company, company)
                .join(user.payments, payment)
                .groupBy(user, company.name)
                .orderBy(payment.amount.avg().desc())
                .fetch();
    }

    /**
     * Возвращает среднюю зарплату по компании
     */
    public List<Tuple> findAllСompaniesWithAvgSalary(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(company.name,
                        payment.amount.avg())
                .from(user)
                .join(user.company, company)
                .join(user.payments, payment)
                .groupBy(company.name)
                .orderBy(payment.amount.avg().desc())
                .fetch();
    }

    /**
     * Возвращает пользователя с одинаковой первой буквой фамилии и компании
     */
    public List<Tuple> findAllWithFirstCharUserLastNameAndCompany(Session session, String character) {
        return new JPAQuery<User>(session)
                .select(user, company.name)
                .from(user)
                .join(user.company, company)
                .where(user.personalInfo.lastname.like(character + "%")
                        .and(user.company.name.like(character + "%")))
                .fetch();
    }

    public static UserDao getInstance() {
        return INSTANCE;
    }
}