#!/bin/bash 
show_menu()
{
	NORMAL=`echo "\033[m"`
	MENU=`echo "\033[00;36m"` #Cyan
	NUMBER=`echo "\033[01;33m"` #Yellow - Bold
	FGRED=`echo "\033[00;41m"`
	RED_TEXT=`echo "\033[00;31m"`
	ENTER_LINE=`echo "\033[00;33m"`
	HEADER=`echo "\033[00;00m"`


	echo -e "${HEADER}===============================================================================================================================================${NORMAL}"

	echo -e "${HEADER}\t\t\t\t\t\t\t  H1B VISA APPLICATION : MENU${NORMAL}"

	echo -e "${HEADER}===============================================================================================================================================${NORMAL}"


	echo -e "\n${NUMBER} 1_A)${MENU} Is the number of petitions with 'DATA ENGINEER' as job title increasing over time?\n${NORMAL}"
	echo -e "${NUMBER} 1_B)${MENU} Find top 5 job titles who are having highest avg growth in applications.\n ${NORMAL}"

	echo -e "${NUMBER} 2_A)${MENU} Which part of the US has the most Data Engineer jobs for each year?\n ${NORMAL}"
	echo -e "${NUMBER} 2_B)${MENU} Find Top 5 locations in the US who have got certified visa for each year.\n ${NORMAL}"

	echo -e "${NUMBER} 3)${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions?\n ${NORMAL}"

	echo -e "${NUMBER} 4)${MENU} Which top 5 employers file the most petitions each year?\n  ${NORMAL}"

	echo -e "${NUMBER} 5)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?\n ${NORMAL}"

	echo -e "${NUMBER} 6)${MENU} Find the percentage and the count of each case status on total applications for each year.\n ${NORMAL}"

	echo -e "${NUMBER} 7)${MENU} Create a bar graph to depict the number of applications for each year.\n ${NORMAL}"

	echo -e "${NUMBER} 8)${MENU} Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). \nArrange the output in descending order.\n ${NORMAL}"

	echo -e "${NUMBER} 9)${MENU} Which are the employers along with the number of petitions who have the success rate more than 70% in petitions.\n (total petitions filed 1000 OR more than 1000)?\n ${NORMAL}"

	echo -e "${NUMBER} 10)${MENU} Which are the job positions along with the number of petitions which have the success rate more than 70% in petitions.\n (total petitions filed 1000 OR more than 1000)?\n ${NORMAL}"

	echo -e "${NUMBER} 11)${MENU} Export result for question no 10 to MySql database.\n ${NORMAL}"

	echo -e "${HEADER}===============================================================================================================================================\n${NORMAL}"
    
	echo -e "${ENTER_LINE} Please enter a menu option and enter or${RED_TEXT} enter to exit. ${NORMAL}"
	read opt
}


function option_picked() 
{
	COLOR='\033[01;32m' # Bold Green
	RESET='\033[00;00m' # Normal White
	MESSAGE="$1"  #modified to post the correct option selected
	echo -e "${COLOR}${MESSAGE}${RESET}"
}


function exit_case() 
{
	COLOR='\033[01;31m' # Bold Red
	RESET='\033[00;00m' # Normal White
	MESSAGE="$1"  #modified to post the correct option selected
	echo -e "${COLOR}${MESSAGE}${RESET}"
}


function user_preference() 
{
	COLOR='\033[01;36m' # Bold Cyan
	RESET='\033[00;00m' # Normal White
	MESSAGE="$1"  #modified to post the correct option selected
	echo -e "${COLOR}${MESSAGE}${RESET}"
}


function clear_old_files() 
{
	COLOR='\033[01;34m' # Bold Blue
	RESET='\033[00;00m' # Normal White
	MESSAGE="$1"  #modified to post the correct option selected
	echo -e "${COLOR}${MESSAGE}${RESET}"
}


function year_menu()
{
	echo -e "${MENU}\n Analysis available for the following options:\n${NORMAL}";
        echo -e "${NUMBER}   -> 2011${MENU} = for the year 2011 ${NORMAL}";
        echo -e "${NUMBER}   -> 2012${MENU} = for the year 2012 ${NORMAL}";
        echo -e "${NUMBER}   -> 2013${MENU} = for the year 2013 ${NORMAL}";
        echo -e "${NUMBER}   -> 2014${MENU} = for the year 2014 ${NORMAL}";
        echo -e "${NUMBER}   -> 2015${MENU} = for the year 2015 ${NORMAL}";
        echo -e "${NUMBER}   -> 2016${MENU} = for the year 2016 ${NORMAL}";
        echo -e "${NUMBER}   -> ${MENU} Press${NUMBER} ENTER${MENU} = for overall analysis ${NORMAL}";

	user_preference "\n Enter your preference: ";
	read year;

	until [[ $year = "" || $year = 2011 || $year = 2012 || $year = 2013 || $year = 2014 || $year = 2015 || $year = 2016 ]];
	do
		user_preference "\n Please enter your choice from the above menu options: ";
		read year;
	done
}




clear
show_menu
while [ opt != '' ]
do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1_A) clear;
	
	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_1a/job1_output/*;
	hadoop fs -rmdir /niit_final_project/query_1a/job1_output;	
	hadoop fs -rm /niit_final_project/query_1a/job2_output/*;
	hadoop fs -rmdir /niit_final_project/query_1a/job2_output;
	clear;
	
	option_picked " 1_A) Is the number of petitions with 'DATA ENGINEER' as job title increasing over time?";

	hadoop jar h1b_query1a.jar Driver1A /niit_final_project/cleaned_data /niit_final_project/query_1a/job2_output;
	
	option_picked "\nOUTPUT - JOB 1\n==============";
	hadoop fs -cat /niit_final_project/query_1a/job1_output/p*;

	option_picked "\nOUTPUT - JOB 2\n==============";
	hadoop fs -cat /niit_final_project/query_1a/job2_output/p*;

	echo -e "";	
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;


        1_B) clear;

	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_1b/job1_output/*;
	hadoop fs -rmdir /niit_final_project/query_1b/job1_output;	
	hadoop fs -rm /niit_final_project/query_1b/job2_output/*;
	hadoop fs -rmdir /niit_final_project/query_1b/job2_output;
	clear;
	
        option_picked "1_B) Find top 5 job titles who are having highest avg growth in applications.";

	hadoop jar h1b_query1b.jar Driver1B /niit_final_project/cleaned_data /niit_final_project/query_1b/job2_output;
	
	option_picked "\nOUTPUT - JOB 1\n==============";
	hadoop fs -cat /niit_final_project/query_1b/job1_output/p*;

	option_picked "\nOUTPUT - JOB 2\n==============";
	hadoop fs -cat /niit_final_project/query_1b/job2_output/p*;

	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;

            
        2_A) clear;
	
	option_picked "2_A) Which part of the US has the most Data Engineer jobs for each year?";
	
	#Display Menu for year selection and read user's choice
	year_menu; 
        
	if [[ $year = "" ]]; then
		option_picked "\nOUTPUT - OVERALL ANALYSIS\n=========================";
		hive -f query2a_all.sql;
	else
		option_picked "\nOUTPUT - $year \n==============";
		hive -hiveconf required_year=$year -f query2a.sql;
	fi

	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;

	
        2_B) clear;

	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_2b/*;
	hadoop fs -rmdir /niit_final_project/query_2b;
	clear;

        option_picked "2_B) Find Top 5 locations in the US who have got certified visa for each year.";
        
	#Display Menu for year selection and read user's choice
	year_menu; 
        
	if [[ $year = "" ]]; then
		pig -x local query2b_all.pig;
		option_picked "\nOUTPUT - OVERALL ANALYSIS\n=========================";
		hadoop fs -cat /niit_final_project/query_2b/p*;
	else
		pig -x local -p required_year=$year -f query2b.pig;
		option_picked "\nOUTPUT - $year \n==============";
		hadoop fs -cat /niit_final_project/query_2b/p*;
	fi


	echo -e ""; 
        exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;

            
	3) clear;

	#clear_old_files "Clearing old output files if any ...";
	#clear;

        option_picked "3) Which industry(SOC_NAME) has the most number of Data Scientist positions?";
	
	option_picked "\nOUTPUT\n=======";
	hive -f query3.sql;
	echo -e "";
	
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
	;;
                  
  
        4) clear;
	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_4/*;
	hadoop fs -rmdir /niit_final_project/query_4;
	clear;

        option_picked "4) Which top 5 employers file the most petitions each year?";

	#Display Menu for year selection and read user's choice
	year_menu; 
        
	if [[ $year = "" ]]; then
		pig -x local query4_all.pig;
		option_picked "\nOUTPUT - OVERALL ANALYSIS\n=========================";
		hadoop fs -cat /niit_final_project/query_4/p*;
	else
		pig -x local -p required_year=$year -f query4.pig;
		option_picked "\nOUTPUT - $year \n==============";
		hadoop fs -cat /niit_final_project/query_4/p*;
	fi


	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;

        
        5) clear;

        option_picked "5) Find the most popular top 10 job positions for H1B visa applications for each year?";
	
        echo -e "\n${NUMBER}   A)${MENU} All Applications (CERTIFIED, CERTIFIED_WITHDRAWN, WITHDRAWN, DENIED) ${NORMAL}";
        echo -e "${NUMBER}   B)${MENU} Certified Applications only ${NORMAL}";

	user_preference "\n Enter your preference: ";
	read ch;

	until [[ $ch = "A" || $ch = "B" ]];
	do
		user_preference "\n Please enter either 'A' or 'B': ";
		read ch;
	done


	#Display Menu for year selection and read user's choice
	year_menu; 

	case $ch in
		A) if [[ $year = "" ]]; then
			option_picked "\nOUTPUT - OVERALL ANALYSIS\n=========================";
			hive -f query5a_all.sql;
		else
			option_picked "\nOUTPUT - $year \n==============";
			hive -hiveconf required_year=$year -f query5a.sql;
		fi
		;;


		B) if [[ $year = "" ]]; then
			option_picked "\nOUTPUT - OVERALL ANALYSIS\n=========================";
			hive -f query5b_all.sql;
		else
			option_picked "\nOUTPUT - $year \n==============";
			hive -hiveconf required_year=$year -f query5b.sql;
		fi
		;;
	esac

	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;

        
	6) clear;

	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_6/job1_output/*;
	hadoop fs -rmdir /niit_final_project/query_6/job1_output;	
	hadoop fs -rm /niit_final_project/query_6/job2_output/*;
	hadoop fs -rmdir /niit_final_project/query_6/job2_output;
	clear;

        option_picked " 6) Find the percentage and the count of each case status on total applications for each year.";

	#Display Menu for year selection and read user's choice
	year_menu; 


	if [[ $year = "" ]]; then
		hadoop jar h1b_query6.jar Driver6 /niit_final_project/cleaned_data /niit_final_project/query_6/job2_output ALL;

	else
		hadoop jar h1b_query6.jar Driver6 /niit_final_project/cleaned_data /niit_final_project/query_6/job2_output $year;
	fi

	option_picked "\nOUTPUT - JOB 1\n==============";
	hadoop fs -cat /niit_final_project/query_6/job1_output/p*;

	option_picked "\nOUTPUT - JOB 2\n==============";
	hadoop fs -cat /niit_final_project/query_6/job2_output/p*;


	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
	;;


	7) clear;

	#clear_old_files "Clearing old output files if any ...";
	#clear;

        option_picked " 7) Create a bar graph to depict the number of applications for each year.";

	hive -f query7.sql;

	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;


	8) clear;

	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_8/job1_output/*;
	hadoop fs -rmdir /niit_final_project/query_8/job1_output;	
	hadoop fs -rm /niit_final_project/query_8/job2_output/*;
	hadoop fs -rmdir /niit_final_project/query_8/job2_output;
	clear;

        option_picked "  8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). \nArrange the output in descending order.";

        echo -e "\n${NUMBER}   A)${MENU} Full Time Job Position ${NORMAL}";
        echo -e "${NUMBER}   B)${MENU} Part Time Job Position ${NORMAL}";

	user_preference "\n Enter your preference: ";
	read ch;
	
	until [[ $ch = "A" || $ch = "B" ]];
	do
		user_preference "\n Please enter either 'A' or 'B': ";
		read ch;
	done


	#Display Menu for year selection and read user's choice
	year_menu; 


	case $ch in
		A) if [[ $year = "" ]]; then
			option_picked "\nOUTPUT - OVERALL ANALYSIS\n=========================";
			hadoop jar h1b_query8.jar Driver8 /niit_final_project/cleaned_data /niit_final_project/query_8/job2_output Y ALL;

		else
			option_picked "\nOUTPUT - $year \n==============";
			hadoop jar h1b_query8.jar Driver8 /niit_final_project/cleaned_data /niit_final_project/query_8/job2_output Y $year;

		fi
		;;


		B) if [[ $year = "" ]]; then
			option_picked "\nOUTPUT - OVERALL ANALYSIS\n=========================";
			hadoop jar h1b_query8.jar Driver8 /niit_final_project/cleaned_data /niit_final_project/query_8/job2_output N ALL;

		else
			option_picked "\nOUTPUT - $year \n==============";
			hadoop jar h1b_query8.jar Driver8 /niit_final_project/cleaned_data /niit_final_project/query_8/job2_output N $year;
		fi
		;;

		*) echo "Please Select either A (Full Time) or B (Part Time)!"
		;;
	esac


	option_picked "\nOUTPUT\n=======";
	hadoop fs -cat /niit_final_project/query_8/job2_output/p*;


	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;


	9) clear;

	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_9/*;
	hadoop fs -rmdir /niit_final_project/query_9;
	clear;

        option_picked " 9) Which are the employers along with the number of petitions who have the success rate more than 70% in petitions.\n (total petitions filed 1000 OR more than 1000)?\n";

	pig -x local query9.pig;
	option_picked "\nOUTPUT \n=======";
	hadoop fs -cat /niit_final_project/query_9/p*;
	

	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;


	10) clear;

	clear_old_files "Clearing old output files if any ...";
	hadoop fs -rm /niit_final_project/query_10/*;
	hadoop fs -rmdir /niit_final_project/query_10;
	clear;

        option_picked " 10) Which are the job positions along with the number of petitions which have the success rate more than 70% in petitions.\n (total petitions filed 1000 OR more than 1000)?\n";

	pig -x local query10.pig;
	option_picked "\nOUTPUT \n=======";
	hadoop fs -cat /niit_final_project/query_10/p*;
	

	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;


	11) clear;
	clear_old_files "Clearing old output files if any ...";
	mysql -u root -p <<CLEAN_QUERY
	USE h1b_project;
	TRUNCATE TABLE query10_output;
	DROP TABLE query10_output;
CLEAN_QUERY

	clear;

        option_picked " 11) Export result for question no 10 to MySql database.";
	
	#Create a table to store the output
	mysql -u root -p <<CREATE_TABLE
	USE h1b_project;
	CREATE TABLE query10_output(job_title VARCHAR(60) NOT NULL, certified_count INT NOT NULL, certified_withdrawn_count INT NOT NULL, total_count INT NOT NULL, success_rate FLOAT NOT NULL, PRIMARY KEY(job_title));
CREATE_TABLE

	#Exporting Query 10 output to MySql using Sqoop
	sqoop export --connect jdbc:mysql://localhost/h1b_project --username root -P --table query10_output --update-mode  allowinsert --update-key job_title   --export-dir /niit_final_project/query_10/p* --input-fields-terminated-by '\t' ;

	option_picked "\nOUTPUT \n=======";

	mysql -u root -p <<DISPLAY_TABLE
	USE h1b_project;
	SELECT * FROM query10_output ORDER BY success_rate DESC;
DISPLAY_TABLE

	echo -e "";
	exit_case "Press ENTER to go back to Menu: ";
	read go_to_menu;
	clear;
	show_menu;
        ;;


        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    	esac
    fi
done

