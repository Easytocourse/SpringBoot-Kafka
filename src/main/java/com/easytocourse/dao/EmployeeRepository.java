package com.easytocourse.dao;

import java.sql.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.easytocourse.model.Employee;

@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Integer>
{
	
}
