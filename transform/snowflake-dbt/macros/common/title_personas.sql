{%- macro title_persona(title) -%}
lower([title]) as lower_title,
  case
  
  -------------------
  -- INFO SECURITY --
  -------------------
  when lower_title in ('chief information security officer', 'vice president & chief information security officer', 'principal security engineer', 'ciso', 'cio')
                      or lower_title LIKE ANY ('%cyber%', '%security%', '%devsecops%', '%ciso%', '%cio%')
                        -- director titles
                        or ((lower_title LIKE ANY ('%director%', '%vp%', '%vice product%') and
                            (lower_title like '%security%')
                          )
  then 'INFO SECURITY'
  
  -----------------------------
--   RELEASE & CHANGE MGMT --
-----------------------------
  
  when (lower_title in ('scrum master')
                       or lower_title like '%quality assurance%' or lower_title like '%qa%'
        --title & rank
                       or ((lower_title like any ('%director%', '%engineer%', '%manager%', '%vp%', '%vice product%') and
                       (lower_title like any ('%test%', '%release%', '%quality%', '%assurance%', '%qa%', '%application delivery%', '%configuration%', '%site reliability%',
                        '%change%', '%deployment%')
                       )
                      )
       )
  then 'RELEASE & CHANGE MGMT'
  
  ------------------------
  -- PROGRAM MANAGEMENT --
  ------------------------
  
  when (lower_title in ('programme manager', 'cso')
                       or lower_title like '%program manager' or lower_title like '%program management' or lower_title like '%service manager%'
                       or lower_title like '%customer success%' or lower_title like '%technical services manager%' or lower_title like '%account director%'
                       or lower_title like '%portfolio manager%' or lower_title like '%customer%engineer%'  or lower_title like '%technical support manager%'
                       or lower_title like '%professional services%' or lower_title like '%business manager%' or lower_title like '%account manager%'
                       or lower_title like '%manager%agile delivery%' or lower_title like '%client manager%' or lower_title like '%project lead%'
--                     rank & title
                       or ((lower_title like '%program%' or lower_title like '%project%' or lower_title like '%technical account%'
                            or lower_title like '%service delivery%'  or lower_title like '%technical service%' or lower_title like '%audit%'
                            or lower_title like '%engagement%' or lower_title like '%analyst%' 
                           ) and
                            (lower_title like '%manager%' or lower_title like '%director%' or lower_title like '%management%' 
                             or lower_title like '%product%' or lower_title like '%engineer%' or lower_title like '%technology%'
                             or lower_title like '%head%'
                           ))) 
                           --ensure no sales
                           and not contains(lower_title, 'sales')
  then 'PROGRAM MANAGEMENT'

------------------------------------
--   'NON-DEVELOPMENT BACK OFFICE --
------------------------------------
    when lower_title in ('accounts payable', 'coo', 'marketing', 'product marketing manager', 'cfo', 
                         'senior order processing supporter', 'billing', 'accounting', 'account executive', 
                         'controller'
                        )
                       --vp titles
                       or ((lower_title like '%vp%' or lower_title like '%vice product%' or lower_title like '%director%' or lower_title like '%manager%') 
                           and
                            (lower_title like '%sales%' or lower_title like '%marketing%' or lower_title like '%people%' or lower_title like '%operations%'
                             or lower_title like '%account%' or lower_title like '%creative%' or lower_title like '%community%' 
                             or lower_title like '%capture%' or lower_title like '%partner%' or lower_title like '%business%'
                             or lower_title like '%event%' or lower_title like '%channel%' or lower_title like '%territory%' 
                            )
                          )
                       or lower_title like '%purchasing%' or lower_title like '%procurement%' or lower_title like '%buyer%'
                       or lower_title like '%contract specialist%' or lower_title like '%legal%' or lower_title like '%lawyer%'
                       or lower_title like '%negotiator%' or lower_title like '%sale%manager' or lower_title like '%partner'
                       or lower_title like '%sourcer%' or lower_title like '%presales consultant'
                       or lower_title like '%chief operating officer%' or lower_title like '%account administrator%'
                       or lower_title like '%partner' or lower_title like '%sale%' or lower_title like '%office manager%' 
                       or lower_title like '%business development%' or lower_title like '%human%' or lower_title like '%account manager%'
                       or lower_title like '%finance%' or lower_title like '%people%' or lower_title like '%payable%' 
                       or lower_title like '%art%' or lower_title like 'head of operations'
                       or lower_title like '%culture%' or lower_title like '%practice%' or lower_title like '%financial officer%'
                       or lower_title like '%contracts administrator%' or lower_title like '%talent acquisition%'
                       or lower_title like '%contracting officer%' or lower_title like '%account executive%' 
                       or lower_title like '%press consultant%' or lower_title like '%license%' 
                       or lower_title like '%financial controller%' or lower_title like '%contracting%'
                       or lower_title like '%business relations%' or lower_title like '%controller %'
                       or lower_title like '%purchaser%' or lower_title like '%office administrator%' 
                       or lower_title like '%accountant%' or lower_title like '%administrative assistant%'
                       or lower_title like '%executive assistant%' or lower_title like '%company administrator%'
                       or lower_title like '%billing%' or lower_title like '%payments%'
                       
  then 'NON-DEVELOPMENT BACK OFFICE'
        

  
  ---------------------------
  -- TECHNOLOGY LEADERSHIP --
  ---------------------------
  
  when ((lower_title in ('chief technology officer', 'cto', 'ceo', 'owner', 'engineering executive', 'president',
                        'co-founder', 'vp', 'avp', 'svp', 'evp', 'director', 'manager', 'cio', 'senior manager', 'board member',
                         'chief technologist', 'président', 'directeur technique', 'it lead', 'head of data', 'executive', 
                         'co-fondateur', 'it executive'
                        )
                       -- rank & titles
                       or ((lower_title like '%vp%' or lower_title like '%vice product%' or lower_title like '%head%' or lower_title like '%chief%') and
                            (lower_title like '%technology%' or lower_title like '%infrastructure%' or lower_title like '% it%' 
                             or lower_title like '% it%' or lower_title like '%product%' or lower_title like '%engineering%'
                             or lower_title like '%app dev%' or lower_title like '%technologist%' or lower_title like '%software%'
                             or lower_title like '%technology%'  or lower_title like '%cloud%' or lower_title like '%r&d%')
                          )
                       or lower_title like '%president%' or lower_title like '%founder%' or lower_title like '%chief executive officer%')
                       or lower_title like '%co-founder%' or lower_title like '%founder%' or lower_title like '%chief information officer%'
                       or lower_title like '%co-owner%' or lower_title like '%unternehmensinhaber %'
                       --only dirrector
                       or (contains(lower_title, 'director') and 
                           (contains(lower_title, 'technology') or contains(lower_title, 'class') or contains(lower_title, 'company') or contains(lower_title, 'it')
                            )
                           ) and (lower_title not in ('director, information technology'))
       )
  then 'TECHNOLOGY LEADERSHIP'
  
---------------------------------------------------
--   PLATFORM / OPS / INFRASTRUCTURE ENGINEERING --
---------------------------------------------------
  when (lower_title in ('chief engineer', 'principal developer', 'founding engineer', 'head of cloud engineering', 
                        'engineering team member', 'devops', 'engineer', 'system administrator', 'technical lead', 
                         'senior engineer', 'cloud arquitect', 'it',
                         'sre', 'head of engineering', 'lead engineer', 'chief engineer', 'staff engineer', 'engineering lead', 
                        'it specialist', 'engineering', 'tech lead', 'associate engineer', 'it manager', 'sre', 
                        'it administrator', 'it functional associate & analyst',
                        'principal', 'gitlab admin', 'it admin', 'principal consultant', 'it associate'
                       )
                        or lower_title like '%architect%' or lower_title like '%devop%' or lower_title like '%technical%' 
                        or lower_title like '%dev ops%' or lower_title like '%network technician%'
                        or lower_title like '%delivery%' or lower_title like '%information technology%' 
                        or lower_title like '%network admin%' or lower_title like '%admin network%' 
                        or lower_title like '%system specialist%' or lower_title like '%it tech%'
                        or lower_title like 'it pro%'
                        or lower_title like '%data science%' or lower_title like '%sre%'  or lower_title like '%site reliability engineer%'
                        or lower_title like '%sysadmin%' or lower_title like '%system admin%' or lower_title like '%systems admin%'
                        or lower_title like '%systems integration%' or lower_title like '%it admin%'
        
                        --manager
                           or ((lower_title like '%manager%' or lower_title like '%director%' or lower_title like '%lead%' or lower_title like '%principal%')                               and 
                                (lower_title like '%engineer%' or lower_title like '%program%' or lower_title like '%information technology%'
                                 or lower_title like ' it ' or lower_title like '%technical%' or lower_title like '%operations%'
                                 or lower_title like '%technology%' or lower_title like '%infrastructure%' or lower_title like '%data%'
                                or lower_title like '%implementation%' or lower_title like '%system%' or lower_title like '%technical%'
                                or lower_title like '%network%')
                              )
                           or ((lower_title like '%engineer%' ) and
                                (lower_title like '%system%' or lower_title like '%data%' or lower_title like '%cloud%' 
                                 or lower_title like '%network%' or lower_title like '%infrastructure%' or lower_title like 'it '
                                 or lower_title like '%platform%' or lower_title like '%automation%' or lower_title like '%technical%'
                                 or lower_title like '%support%' or lower_title like '%research%' or lower_title like '%electrical%'
                                 or lower_title like '%solutions%' or lower_title like '%operations%' or lower_title like '%process%'
                                )
                              )
                        )
  then 'PLATFORM / OPS / INFRASTRUCTURE ENGINEERING'
        
  ---------------------
  -- APP DEVELOPMENT --
  ---------------------
  when (lower_title in ('product team member', 'technical lead', 'product owner', 'project engineer', 'business analyst',
                        'engineering lead', 'head of software engineering', 'project lead developer',
                        'lead platform engineer', 'lead qa engineer', 'product designer', 'cpo', 'product executive',
                        'lead automation engineer', 'devops engineer', 'head of development', 'head of software development')
                       or lower_title like '%product owner%' 
                       
                       or lower_title like '%manager%' or lower_title like '%director%' 
                       or lower_title like '%team lead%' 
                       -- title and rank
                       or (lower_title like '%head of%' or lower_title = 'vp' or lower_title like '%vice president%'
                          or lower_title like '%directeur%') and (
                         lower_title like '%software development%' or lower_title like '%platform engineering%' 
                         or lower_title like '%data engineering%' or lower_title like '% engineering' 
                         or lower_title like '% software' or lower_title like '%software development'
                         or lower_title like '%software' or lower_title like '%produits%'
                         or lower_title like '%web development%' 

                       )
       )
  then 'APP DEVELOPMENT'  
  
  ------------------
  -- IC DEVELOPER --
  ------------------
  when (lower_title in ('dev', 'system administrator', 'programmer', 'sre', 'programer', 'dba', 'data scientist', 'dev lead',
                        'analyst', 'system analyst') 
                       or lower_title like '%developer%' or lower_title like '%engineer%' or lower_title like '%engineering%'
                       or lower_title like '%software specialist%' 
                       or lower_title like '%programmer%' or lower_title like '%programador%' 
                       or lower_title like '%systems analyst%' or lower_title like '%database administrator%'
                       or lower_title like '%web designer%' or lower_title like '%data analyst%' 
                       or lower_title like '%data scientist%' or lower_title like '%software lead%'
                       or lower_title like '%computer scientist%' or lower_title like '%fellow%'
                       or lower_title like '%researcher' or lower_title like '%researh'
                       or lower_title like '%software development%'
       )
  

  
  then 'IC DEVELOPER'
  
  -----------
  -- OTHER --
  -----------
  
  when lower_title in ('student', 'other', 'none', 'qa', 'professor', 'researcher', 'team member', 'research assistant',
        'instructor', 'personal email', 'sold to contact', 'experiment default value - signup not completed', 'lecturer',
        'drupal 8', 'realtor', 'no longer at company', 'shipping address', 'po', 'us'
        )
        or lower_title like '%intern%' or lower_title like '%professor%' or lower_title like '%postdoctoral%' 
        or lower_title like '%teacher%'
  then 'OTHER'
  
  -----------------
  -- Blank Value --
  -----------------
  
  when lower_title in ('-', null, 'n/a', 'na', 'test', 'mr', 's', 'other', 'mr.', 'd')
  then 'Blank Value' 
  
  -- when lower_title in ('it', 'data analyst', 'it specialist', 'consultant', 'systems administrator', 'analyst', 'senior consultant',
                       -- 'data scientist', 'project engineer', 'program analyst', 'programming analyst', 'program specialist', 'program coordinator')
  -- then 'Review' 
  end as title_persona
{%- endmacro -%}

