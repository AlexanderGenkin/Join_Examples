using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using TestProject.DataModel.Crm.Entities;

namespace Join_Examples
{
    public class Class1
    {
        public void QueryExpressionExample1(IOrganizationService _service)
        {

//SELECT lead.FullName
//FROM Leads as lead
//LEFT OUTER JOIN Tasks as ab
//ON(lead.leadId = ab.RegardingObjectId)
//WHERE ab.RegardingObjectId is null


            QueryExpression qx = new QueryExpression("lead");
            qx.ColumnSet.AddColumn("subject");

            LinkEntity link = qx.AddLink("task", "leadid", "regardingobjectid", JoinOperator.LeftOuter);
            link.Columns.AddColumn("subject");
            link.EntityAlias = "tsk";

            qx.Criteria = new FilterExpression();
            qx.Criteria.AddCondition("tsk", "activityid", ConditionOperator.Null);

        }

        public void QueryExpressionExample2(IOrganizationService _service)
        {
            // Build the following SQL query using QueryExpression:
            //
            //      SELECT contact.fullname, contact.address1_telephone1
            //      FROM contact
            //          LEFT OUTER JOIN account
            //              ON contact.parentcustomerid = account.accountid
            //              AND
            //              account.name = 'Litware, Inc.'
            //      WHERE (contact.address1_stateorprovince = 'WA'
            //      AND
            //          contact.address1_city in ('Redmond', 'Bellevue', 'Kirkland', 'Seattle')
            //      AND
            //          contact.address1_telephone1 like '(206)%'
            //          OR
            //          contact.address1_telephone1 like '(425)%'
            //      AND
            //          DATEDIFF(DAY, contact.createdon, GETDATE()) > 0
            //      AND
            //          DATEDIFF(DAY, contact.createdon, GETDATE()) < 30
            //      AND
            //          contact.emailaddress1 Not NULL
            //             )

            QueryExpression query = new QueryExpression()
            {
                Distinct = false,
                EntityName = Contact.EntityLogicalName,
                ColumnSet = new ColumnSet("fullname", "address1_telephone1"),
                LinkEntities =
    {
        new LinkEntity
        {
            JoinOperator = JoinOperator.LeftOuter,
            LinkFromAttributeName = "parentcustomerid",
            LinkFromEntityName = Contact.EntityLogicalName,
            LinkToAttributeName = "accountid",
            LinkToEntityName = Account.EntityLogicalName,
            LinkCriteria =
            {
                Conditions =
                {
                    new ConditionExpression("name", ConditionOperator.Equal, "Litware, Inc.")
                }
            }
        }
    },
                Criteria =
    {
        Filters =
        {
            new FilterExpression
            {
                FilterOperator = LogicalOperator.And,
                Conditions =
                {
                    new ConditionExpression("address1_stateorprovince", ConditionOperator.Equal, "WA"),
                    new ConditionExpression("address1_city", ConditionOperator.In, new String[] {"Redmond", "Bellevue" , "Kirkland", "Seattle"}),
                    new ConditionExpression("createdon", ConditionOperator.LastXDays, 30),
                    new ConditionExpression("emailaddress1", ConditionOperator.NotNull)
                },
            },
            new FilterExpression
            {
                FilterOperator = LogicalOperator.Or,
                Conditions =
                {
                    new ConditionExpression("address1_telephone1", ConditionOperator.Like, "(206)%"),
                    new ConditionExpression("address1_telephone1", ConditionOperator.Like, "(425)%")
                }
            }
        }
    }
            };

            DataCollection<Entity> entityCollection = _service.RetrieveMultiple(query).Entities;

            // Display the results.
            Console.WriteLine("List all contacts matching specified parameters");
            Console.WriteLine("===============================================");
            foreach (Contact contact in entityCollection)
            {
                Console.WriteLine("Contact ID: {0}", contact.Id);
                Console.WriteLine("Contact Name: {0}", contact.FullName);
                Console.WriteLine("Contact Phone: {0}", contact.Address1_Telephone1);
            }
            Console.WriteLine("[End of Listing]");
            Console.WriteLine();
        }

        public void LinqExample1(IOrganizationService _service)
        {

            using (ServiceContext context = new ServiceContext(_service))
            {
                var leads = context.LeadSet;
                var users = context.SystemUserSet;

                var query =
                    from lead in leads
                    join user in users 
                    on lead.CreatedBy.Id equals user.SystemUserId
                    where user.FirstName == "Alexander"
                    && lead.CreatedOn.Value.Month == 8
                    select new
                    {
                        LeadId = lead.LeadId,
                        CreatorName = user.FirstName + " " + user.LastName,
                        CreationDate = lead.CreatedOn,
                    };

                foreach (var lead in query)
                {
                    Console.WriteLine("{0}\t{1}\t{2:d}\t{3}",
                        lead.LeadId,
                        lead.CreatorName,
                        lead.CreationDate);
                }
            }
//            LINQ Operator   Limitations
//join    Represents an inner or outer join. Only left outer joins are supported.
//from Supports one from clause per query.
//where The left side of the clause must be a column name and the right side of the clause
//must be a value.You cannot set the left side to a constant.Both the sides of the
//clause cannot be constants.

//Supports the String functions Contains, StartsWith, EndsWith, and Equals.
//groupBy Not supported.FetchXML supports grouping options that are not available with
//the LINQ query provider.More information: Use FetchXML Aggregation

//orderBy Supports ordering by table columns, such as Contact.FullName.
//select  Supports anonymous types, constructors, and initializers.
//last The last operator is not supported.
//skip and take Supports skip and take using server-side paging.The skip value must be greater than
//or equal to the take value.
//aggregate Not supported.FetchXML supports aggregation options that
//are not available with the LINQ query provider.
//More information: Use FetchXML Aggregation



        }






//FetchXML Agregation Limitations
//Queries that return aggregate values are limited to 50,000 records.
//If the filter criteria in your query includes more than 50,000 records you will get error


        public void FetchXMLExample1(IOrganizationService _service)
        {

            // Retrieve all accounts owned by the user with read access rights to the accounts and   
            // where the last name of the user is not Alexander.   
            string fetch2 = @"  
                <fetch mapping='logical'>  
                     <entity name='account'>   
                        <attribute name='accountid'/>   
                        <attribute name='name'/>   
                        <link-entity name='systemuser' to='owninguser'>   
                           <filter type='and'>   
                              <condition attribute='lastname' operator='ne' value='Alexander' />   
                           </filter>   
                        </link-entity>   
                     </entity>   
                   </fetch> ";

            EntityCollection result = _service.RetrieveMultiple(new FetchExpression(fetch2));
            foreach (var c in result.Entities)
            {
                System.Console.WriteLine(c.Attributes["name"]);
            }
        }


        public void FetchXMLExample2(IOrganizationService _service)
        {
            // Fetch the count of all opportunities.  This is the equivalent of
            // SELECT COUNT(*) AS opportunity_count ... in SQL.
            string opportunity_count = @" 
                <fetch distinct='false' mapping='logical' aggregate='true'> 
                   <entity name='opportunity'> 
                      <attribute name='name' alias='opportunity_count' aggregate='count'/> 
                   </entity> 
                </fetch>";

            EntityCollection opportunity_count_result = _service.RetrieveMultiple(new FetchExpression(opportunity_count));

            foreach (var c in opportunity_count_result.Entities)
            {
                Int32 aggregate2 = (Int32)((AliasedValue)c["opportunity_count"]).Value;
                System.Console.WriteLine("Count of all opportunities: " + aggregate2);

            }

            // Fetch the number of opportunities each manager's direct reports 
            // own using a groupby within a link-entity.
            string groupby2 = @" 
                <fetch distinct='false' mapping='logical' aggregate='true'> 
                   <entity name='opportunity'> 
                      <attribute name='name' alias='opportunity_count' aggregate='countcolumn' /> 
                      <link-entity name='systemuser' from='systemuserid' to='ownerid'>
                          <attribute name='parentsystemuserid' alias='managerid' groupby='true' />
                      </link-entity> 
                   </entity> 
                </fetch>";

            EntityCollection groupby2_result = _service.RetrieveMultiple(new FetchExpression(groupby2));

            foreach (var c in groupby2_result.Entities)
            {

                int? aggregate10a = (int?)((AliasedValue)c["opportunity_count"]).Value;
                System.Console.WriteLine("Count of all opportunities: " + aggregate10a + "\n");
            }

        }

    }
}
