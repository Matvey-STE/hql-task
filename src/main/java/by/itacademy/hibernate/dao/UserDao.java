package by.itacademy.hibernate.dao;


import by.itacademy.hibernate.entity.Payment;
import by.itacademy.hibernate.entity.User;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hibernate.Session;

import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserDao {

    private static final UserDao INSTANCE = new UserDao();

    /**
     * Возвращает всех сотрудников
     */
    public List<User> findAll(Session session) {
        return session.createQuery("select u from User u", User.class).list();
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

    public static UserDao getInstance() {
        return INSTANCE;
    }
}