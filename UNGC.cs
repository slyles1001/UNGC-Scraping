using HtmlAgilityPack;
using Npgsql;
using ScrapySharp.Extensions;
using ScrapySharp.Network;
using System;
using System.Collections.Generic;
//using System.Diagnostics;
using System.Globalization;
using System.Linq;
//using System.Runtime.CompilerServices;
using System.Web;

namespace UNGC
{      // Parses HTML data into PostgreSQL database; No rest APIs for UNGC

    public class DB_row {
        //container class for the data on each member page
        public string name { get; set; }
        public string date_joined { get; set; }
        public string date_due { get; set; }
        
        public string country { get; set; }
        public string org_type { get; set; }
        public string sector { get; set; }
        
        public string status { get; set; }
        public string employees { get; set; }
        public string ownership { get; set; }

        public string[] return_all()
        {   // return a string array of the values scraped; need to run fill first
            string[] output = new string[9] {name, date_joined, date_due, country, org_type, sector, status, employees, ownership };
            return output;
        }

        private static string escape(string txt)
        {   // returns a cleaned string for the sqls
            List<char> needed = new List<char> { '\'', ',', '(', ')' }; // leave these chars in names, countries, etc.
            txt = new string(txt.Where(c => !char.IsPunctuation(c) || needed.Contains(c)).ToArray());
            txt = txt.Replace("'", "''"); // want to enter apostrophe into psql
            //Debug.WriteLine(txt);
            return txt;
        }

        public void fill(string[] data) 
        {   // fill the DB_row class object with data (pulled from web page)
            name = escape(data[0]);
            date_joined = escape(data[1]);
            date_due = escape(data[2]);
            country = escape(data[3]);
            org_type = escape(data[4]);
            sector = escape(data[5]);
            status = escape(data[6]);
            employees = escape(data[7]);
            ownership = escape(data[8]);
        }
    }
    public class UNGC_DB
    {   // class object to scrape data
        public DB_row link_info(string url)
        {   // in: url of member page; out: DB_row object (really a string[] of scraped data)
            string[] data = new string[9]; string temp; // holds scraped data to input into DB_row; tmp string
            DB_row output = new DB_row();   // variable to return
            ScrapingBrowser Browser = new ScrapingBrowser();    // browser to open link
            WebPage PageResult = Browser.NavigateToPage(new Uri(url), 0, "", null); // open page
            data[0] = PageResult.Html.CssSelect(".main-content-header").First<HtmlNode>().SelectSingleNode("h1").InnerText; //  get name
            temp = PageResult.Html.CssSelect(".company-information-since").First<HtmlNode>().SelectSingleNode("time").InnerText;    // get join date
            data[1] = DateTime.Parse(temp, CultureInfo.InvariantCulture).ToString("yyyyMMdd", CultureInfo.InvariantCulture);    //clean join date
            temp = PageResult.Html.CssSelect(".company-information-cop-due").First<HtmlNode>().SelectSingleNode("time").InnerText;  // get due/leave date
            data[2] = DateTime.Parse(temp, CultureInfo.InvariantCulture).ToString("yyyyMMdd", CultureInfo.InvariantCulture);    // clean due/leave date

            HtmlNode table = PageResult.Html.CssSelect(".company-information-overview").First<HtmlNode>();  // hold rest of data
            int i = 3;  // iterator for data[]
            HtmlNode row = table.SelectSingleNode("dl");    // data is under dl node
            HtmlNodeCollection item_nm = row.SelectNodes("dt"); // data name is under dt; "name:", "type:", etc
            HtmlNodeCollection item_val = row.SelectNodes("dd");    //  data val is under dd; "wal-mart", "company", etc
            for (int j = 0; j < item_nm.Count; j++)
            {   // most of the members are either active or haven't given a reason for delisting, so we want to ignore this for now
                if (item_nm[j].InnerText != "Reason for Delisting:")
                {
                    data[i] = HttpUtility.HtmlDecode(item_val[j].InnerText);    // pull data if not reason
                    i++;
                }
            }
            
            output.fill(data);  // enter (and escape) data pulled from page
            return output;
        }

        public List<string[]> get_page(string url)
        {   // in: url of participant list page; out: List<string> containing data from each member
            ScrapingBrowser Browser = new ScrapingBrowser();
            WebPage PageResult = Browser.NavigateToPage(new Uri(url), 0, "", null);
            string base_url = "https://www.unglobalcompact.org";
            List<string[]> page_data = new List<string[]>();
            HtmlNode[] array = PageResult.Html.CssSelect(".participants-table").ToArray<HtmlNode>();    // hold all table entries
            UNGC_DB li = new UNGC_DB(); // object to call link_info (make static?)

            for (int j = 0; j < array.Length; j++)
            {
                foreach (HtmlNode row in array[j].SelectNodes("tbody/tr"))  //  for each member in table
                {
                    string next_link = row.SelectSingleNode("th/a").Attributes["href"].Value;   // grab link to member's page
                    string[] scrape = li.link_info(base_url + next_link).return_all();  // grab string[] from returned object
                    page_data.Add(scrape);  // add member's data to list
                }
            }
            return page_data;
        }
        public static int find_page_count(string url)
        {   //  in: url of first page of member list; out: number of pages
            ScrapingBrowser Browser = new ScrapingBrowser();
            WebPage PageResult = Browser.NavigateToPage(new Uri(url), 0, "", null);
            string num_entries = PageResult.Html.CssSelect(".results-count").First<HtmlNode>().SelectSingleNode("strong").InnerText;

            int num_pages = (int)Math.Ceiling(decimal.Parse(num_entries) / 50);

            return num_pages;
        }
        public static void scrape_data(string url, NpgsqlCommand cmd)
        {
            string[] url_pieces = url.Split(new char[] { '1' });
            UNGC_DB from_html = new UNGC_DB();
            int pg_count = find_page_count(url);
            //Debug.WriteLine(pg_count);
            for (int i = 0; i < pg_count; i++)
            {
                Console.WriteLine(i);
                url = url_pieces[0] + (i + 1).ToString() + url_pieces[1];
                List<string[]> scraped = from_html.get_page(url);
                UNGC_DB.enter_data(scraped, cmd);
            }
        }

        public static void enter_data(List<string[]> scraped, NpgsqlCommand cmd)
        {
            // scraped is a list so it has the foreach method built into it
            scraped.ForEach(delegate (string[] row)
            {
                cmd.CommandText = string.Format("INSERT INTO UNGC SELECT '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}' WHERE not EXISTS (select name from UNGC where name = '{0}');", row);
                cmd.ExecuteNonQuery();
            });
        }

        public static void UNGC_DB_entries(string url)
        {
            string connectstring = "Host=localhost;Username=Seth;Database=ungc_test;Password=1234";
            using (NpgsqlConnection conn = new NpgsqlConnection(connectstring)) // connect to our db
            {
                conn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand()) // open a new command string
                {
                    cmd.Connection = conn;
                    string[] fields = new string[] {"NAME", "DATE_JOINED","DATE_DUE",   // fields in UNGC data
                            "COUNTRY","ORG_TYPE","SECTOR","STATUS","EMPLOYEES","OWNERSHIP"};
                    cmd.CommandText = string.Format("CREATE TABLE IF NOT EXISTS UNGC({0} varchar(250), {1} date, {2} date, {3} varchar(150), {4} varchar(150), {5} varchar(150), {6} varchar(150), {7} int, {8} varchar(150));", fields);
                    cmd.ExecuteNonQuery();  // Create and execute database command
                    UNGC_DB.scrape_data(url, cmd);
                }
            }
        }

        public static void test_db(NpgsqlCommand cmd)
        {   // if we want to query
            cmd.CommandText = "SELECT employees FROM UNGC;";
            using (NpgsqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0));
                }
            }
        }

        public static int Main()
        {
            string url = "https://www.unglobalcompact.org/what-is-gc/participants/search?page=1&search%5Bkeywords%5D=&search%5Bper_page%5D=50&search%5Bsort_direction%5D=asc&search%5Bsort_field%5D=&utf8=%E2%9C%93";
            UNGC_DB.UNGC_DB_entries(url);
            return 0;
        }
    }
}