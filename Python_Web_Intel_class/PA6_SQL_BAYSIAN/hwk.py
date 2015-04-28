#!/usr/local/bin/python
import sqlite3

homework = sqlite3.connect(':memory:')
c = homework.cursor()

# Create database for Bayesian network

c.execute('CREATE TABLE a (a text, p float)')
c.execute('INSERT INTO a VALUES ("Y", 0.01)')
c.execute('INSERT INTO a VALUES ("N", 0.99)')
homework.commit()

c.execute('CREATE TABLE t (t text, a text, p float)')
c.execute('INSERT INTO t VALUES ("Y", "Y", 0.05)')
c.execute('INSERT INTO t VALUES ("N", "Y", 0.95)')
c.execute('INSERT INTO t VALUES ("Y", "N", 0.01)')
c.execute('INSERT INTO t VALUES ("N", "N", 0.99)')
homework.commit()

c.execute('CREATE TABLE s (s text, p float)')
c.execute('INSERT INTO s VALUES ("Y", 0.50)')
c.execute('INSERT INTO s VALUES ("N", 0.50)')
homework.commit()

c.execute('CREATE TABLE l (l text, s text, p float)')
c.execute('INSERT INTO l VALUES ("Y", "Y", 0.10)')
c.execute('INSERT INTO l VALUES ("N", "Y", 0.90)')
c.execute('INSERT INTO l VALUES ("Y", "N", 0.01)')
c.execute('INSERT INTO l VALUES ("N", "N", 0.99)')
homework.commit()

c.execute('CREATE TABLE b (b text, s text, p float)')
c.execute('INSERT INTO b VALUES ("Y", "Y", 0.60)')
c.execute('INSERT INTO b VALUES ("N", "Y", 0.40)')
c.execute('INSERT INTO b VALUES ("Y", "N", 0.30)')
c.execute('INSERT INTO b VALUES ("N", "N", 0.70)')
homework.commit()

c.execute('CREATE TABLE e (e text, l text, t text, p float)')
c.execute('INSERT INTO e VALUES ("N", "N", "N", 1.00)')
c.execute('INSERT INTO e VALUES ("N", "N", "Y", 0.00)')
c.execute('INSERT INTO e VALUES ("N", "Y", "N", 0.00)')
c.execute('INSERT INTO e VALUES ("N", "Y", "Y", 0.00)')
c.execute('INSERT INTO e VALUES ("Y", "N", "N", 0.00)')
c.execute('INSERT INTO e VALUES ("Y", "N", "Y", 1.00)')
c.execute('INSERT INTO e VALUES ("Y", "Y", "N", 1.00)')
c.execute('INSERT INTO e VALUES ("Y", "Y", "Y", 1.00)')
homework.commit()

c.execute('CREATE TABLE x (x text, e text, p float)')
c.execute('INSERT INTO x VALUES ("N", "N", 0.95)')
c.execute('INSERT INTO x VALUES ("N", "Y", 0.02)')
c.execute('INSERT INTO x VALUES ("Y", "N", 0.05)')
c.execute('INSERT INTO x VALUES ("Y", "Y", 0.98)')
homework.commit()

c.execute('CREATE TABLE d (d text, e text, b text, p float)')
c.execute('INSERT INTO d VALUES ("N", "N", "N", 0.90)')
c.execute('INSERT INTO d VALUES ("N", "N", "Y", 0.20)')
c.execute('INSERT INTO d VALUES ("N", "Y", "N", 0.30)')
c.execute('INSERT INTO d VALUES ("N", "Y", "Y", 0.10)')
c.execute('INSERT INTO d VALUES ("Y", "N", "N", 0.10)')
c.execute('INSERT INTO d VALUES ("Y", "N", "Y", 0.80)')
c.execute('INSERT INTO d VALUES ("Y", "Y", "N", 0.70)')
c.execute('INSERT INTO d VALUES ("Y", "Y", "Y", 0.90)')
homework.commit()

print
print "Chance of Tuberculosis with no Asia, smoking, Xray, or Dys:"

c.execute('SELECT t.t, SUM(a.p *b.p *d.p *e.p *l.p *s.p *t.p *x.p) \
 	FROM a,b,d,e,l,s,t,x \
	WHERE t.a=a.a AND l.s=s.s AND b.s=s.s \
	AND e.t=t.t AND e.l=l.l \
	AND x.e=e.e AND d.e=e.e \
	AND d.b=b.b\
	AND a.a="N" \
	AND x.x="Y" \
	GROUP BY t.t')

raw = c.fetchall()
print raw

res_neg = raw[0][1]
res_pos = raw[1][1]

result = 100.0 * res_pos / (res_pos + res_neg)
print "Py", result
print "Pn", 100.0 - result
print
