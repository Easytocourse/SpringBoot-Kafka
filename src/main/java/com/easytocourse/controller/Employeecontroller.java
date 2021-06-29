package com.easytocourse.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.easytocourse.model.Employee;
import com.easytocourse.service.Employeeservice;

@RestController
@RequestMapping("/Employee")
public class Employeecontroller {

	@Autowired
	Employeeservice empservice;
	
	@GetMapping("/testloop") 
	public void sendingloop(){
		empservice.sendingloop();
		
	}
	
	@GetMapping("/testsingletopic") 
	public void sendingsingletopic(){
		empservice.sendingtosingletopic();
		
	}
	
	@GetMapping("/testmultitopic") 
	public void sendingmultitopic(){
		empservice.sendingtomultitopic();
		
	}
	@GetMapping("/testreply") 
	public void listenandreply(){
		empservice.sendingtofromtopic();
		
	}
	
	@PostMapping("/test") 
	public String testkafka(@RequestParam("message") String message){
		return empservice.sendtokafka(message);
		
	}
	
	@PostMapping("/addAction") 
	public Employee addemployee(@RequestBody Employee emp){
		return empservice.saveaction(emp);
		
	}
	@GetMapping("/getActions")
	public List<Employee> getactions()
	{
		return empservice.getallactionitems();
		
	}
}
