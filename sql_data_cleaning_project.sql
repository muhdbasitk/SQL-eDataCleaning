/*
 
 Cleaning Data with SQL Queries

*/

------------------------------------------------
--Get to Know the Data 

--view the data 
select * from nashville_housing_data
limit 1000;

--see number of rows 
select count(*) from nashville_housing_data;

------------------------------------------------
--Populate Property Address Data 

--find total null values in propertyaddress column 
select count(*)
from nashville_housing_data
where propertyaddress is null;  

--create a view to copy original data
create view housing_data_b as 
select * from nashville_housing_data;

--create a view where missing property addresses are replaced by appropriate address
create view new_values as 
with cte as 
	(
	select row_number() over(partition by a.uniqueid order by a.uniqueid) as rn , 
		a.uniqueid as unique_a, 
		a.parcelid as parcel_a, 
		a.propertyaddress as property_a, 
		b.uniqueid as unique_b, 
		b.parcelid as parcel_b, 
		b.propertyaddress as property_b, 
		coalesce(a.propertyaddress, b.propertyaddress) as replacement
	from nashville_housing_data a
		left join housing_data_b b 
		on a.parcelid = b.parcelid
		and a.uniqueid <> b.uniqueid
		where a.propertyaddress is null
	)
select unique_a, parcel_a, property_a, unique_b, parcel_b, property_b, replacement
from cte
where rn = 1;  

-- another way to look at the data
with cte as 
	(
	select row_number() over(partition by a.uniqueid order by a.uniqueid) as rn , 
		a.uniqueid as unique_a, 
		a.parcelid as parcel_a, 
		a.propertyaddress as property_a, 
		b.uniqueid as unique_b, 
		b.parcelid as parcel_b, 
		b.propertyaddress as property_b, 
		coalesce(a.propertyaddress, b.propertyaddress) as replacement
	from nashville_housing_data a
		left join housing_data_b b 
		on a.parcelid = b.parcelid
		and a.uniqueid <> b.uniqueid
		where a.propertyaddress is null
	)
select b.unique_a, b.parcel_a, b.property_a, b.unique_b, b.parcel_b, b.property_b, b.replacement, a.propertyaddress
from nashville_housing_data a
inner join cte b on a.uniqueid = b.unique_a
where rn = 1; 				

--replace the values with correct data using the view we created earlier
update nashville_housing_data a
	set a.propertyaddress = b.replacement
	from new_values b
	where propertyaddress is null and a.uniqueid = b.unique_a; 

	
------------------------------------------------
--Split both address columns into individual columns (Address, City, State)

--view the data 
select propertyaddress, owneraddress
from nashville_housing_data; 

select count(*)
from nashville_housing_data 
where owneraddress is null;

--split propertyaddress data and add split data into two new columns
select 
	trim(substr(propertyaddress, 1, locate(',',propertyaddress)-1)) as address,
	trim(substr(propertyaddress, locate(',',propertyaddress)+1, length(propertyaddress))) as city
from nashville_housing_data;

alter table nashville_housing_data 
add column short_address varchar(255)
add column property_city varchar(255);

update nashville_housing_data 
set short_address = trim(substr(propertyaddress, 1, locate(',',propertyaddress)-1));

update nashville_housing_data 
set property_city = trim(substr(propertyaddress, locate(',',propertyaddress)+1, length(propertyaddress)));

--check your work 
select propertyaddress, short_address, property_city
from nashville_housing_data;

--split owneraddress data and add split data into two new columns
select 
	owneraddress,
	trim(substr(owneraddress, 1, locate(',',owneraddress)-1)) as address,
	trim(substr(owneraddress, locate(',',owneraddress)+1, length(owneraddress)-locate(',',owneraddress)-4)) as city,
	trim(right(owneraddress,2)) as state
from nashville_housing_data;

alter table nashville_housing_data 
add column owner_address_short varchar(255)
add column owner_city varchar(255)
add column owner_state varchar(255);

update nashville_housing_data 
set owner_address_short = trim(substr(owneraddress, 1, locate(',',owneraddress)-1));

update nashville_housing_data 
set owner_city = trim(substr(owneraddress, locate(',',owneraddress)+1, length(owneraddress)-locate(',',owneraddress)-4));

update nashville_housing_data 
set owner_state = trim(right(owneraddress,2));

--check you work 
select owneraddress, owner_address_short, owner_city, owner_state
from nashville_housing_data;


------------------------------------------------
--Change Y and N to Yes and No in 'soldasvacant' field

--view the distinct values in the column 
select distinct soldasvacant, count(*) as count
from nashville_housing_data
group by soldasvacant
order by count;

-- update Y and N strings accordingly. 
select soldasvacant, 
	case 
		when soldasvacant = 'Y' then 'Yes'
		when soldasvacant = 'N' then 'No'
		else soldasvacant
	end as yes_no
from nashville_housing_data;

update nashville_housing_data 
set soldasvacant = case 
		when soldasvacant = 'Y' then 'Yes'
		when soldasvacant = 'N' then 'No'
		else soldasvacant
	end;

--check you work 
select distinct soldasvacant, count(*) as count 
from nashville_housing_data
group by soldasvacant
order by count;

------------------------------------------------
--Check if saleprice column has null values

select saleprice 
from nashville_housing_data 
where saleprice is null; --found no null values


------------------------------------------------
--Remove duplicates and drop unnecessary columns

--check to see how many duplicate values there are

with cte as 
	(
	select *, 
		row_number() over( 
			partition by 
				parcelid, 
				landuse,
				propertyaddress,
				saledate, 
				saleprice, 
				legalreference,
				soldasvacant,
				ownername,
				owneraddress,
				acreage,
				taxdistrict,
				landvalue,
				buildingvalue,
				totalvalue,
				yearbuilt,
				bedrooms,
				fullbath,
				halfbath
			order by uniqueid
			) rn
	from nashville_housing_data
	)
select * 
from cte
where rn > 1;

--remove duplicate rows and remove unnecessary columns using a view

create or replace view clean_nashville_data as 
with cte as 
	(
	select *, 
		row_number() over( 
			partition by 
				parcelid, 
				landuse,
				propertyaddress,
				saledate, 
				saleprice, 
				legalreference,
				soldasvacant,
				ownername,
				owneraddress,
				acreage,
				taxdistrict,
				landvalue,
				buildingvalue,
				totalvalue,
				yearbuilt,
				bedrooms,
				fullbath,
				halfbath
			order by uniqueid
			) rn
	from nashville_housing_data
	)
select uniqueid, parcelid,landuse,propertyaddress,saledate, saleprice, legalreference,soldasvacant,ownername,acreage,
		landvalue,buildingvalue,totalvalue,yearbuilt,bedrooms,fullbath,halfbath, short_address as property_address_short, 
		property_city, owner_address_short, owner_city, owner_state
from cte
where rn = 1;


--check your work 
with cte as 
	(
	select *, 
		row_number() over( 
			partition by 
				parcelid, 
				landuse,
				propertyaddress,
				saledate, 
				saleprice, 
				legalreference,
				soldasvacant,
				ownername,
				acreage,
				landvalue,
				buildingvalue,
				totalvalue,
				yearbuilt,
				bedrooms,
				fullbath,
				halfbath
			) row_number
	from clean_nashville_data
	)
select * 
from cte
where row_number > 1;
------------------------------------------------
--View clean dataset

select * from clean_nashville_data;

------------------------------------------------
------------------------------------------------