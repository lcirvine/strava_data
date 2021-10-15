# strava_data #
This project uses the [Strava API](https://developers.strava.com/docs/reference/) to download activity data for an athlete so that the data can be analysed. The data is then stored in a [PostgreSQL database](https://github.com/lcirvine/strava_data/blob/cfa720274e5f312310a633a824a345375894a54e/reference/strava_db.png), however, once the data is downloaded it could be saved in other formats. Once all the data has been stored, users can analyse the data and create interesting visualizations (example below).

## Requirements ##
1. Please see the required Python [libraries](https://github.com/lcirvine/strava_data/blob/cfa720274e5f312310a633a824a345375894a54e/requirements.txt)
2. You'll need a Strava account and to register to use the Strava API - [Strava's getting started guide](https://developers.strava.com/docs/getting-started/)
3. Since the activity data is saved in a [PostgreSQL](https://www.postgresql.org/) database, you'll also need to [download PostgreSQL](https://www.postgresql.org/download/)

## Why I Made This ##
I love running and data analysis so this was a perfect project for me! I created this primarily as a learning exercise. Through this project I was able to practice using APIs, Python (especially the pandas, matplotlib and seaborn libraries) and PostgreSQL. And as a bonus, I now have all my running data! 

## Contributing ##
If you'd like to contribute, pull requests are welcome!

## Data Analysis Example ##
Like many people, I live in an area which had lockdowns during the worst of the pandemic. At times the only time I left the house was to go for a run. At the same time I was working from home and didn't have the usual school run in the morning. That meant that, as long as I didn't have any meetings or urgent deadlines, I could take a break for an hour during the day and go for a run. That made me curious if I could see the effect that had on when I went running. 

I found that during 2019 I consistently ran between 6-7am during the week and on Saturday mornings. In contrast, during 2020 my runs were much more spread out. I also ran at later times and typically ran later in the day as the week went on. 

### 2019 Time of Day Heatmap ###
![2019 Time of Day Heatmap](https://github.com/lcirvine/strava_data/blob/cfa720274e5f312310a633a824a345375894a54e/reference/2019%20Time%20of%20Day%20Heatmap.png)

### 2020 Time of Day Heatmap ###
![2020 Time of Day Heatmap](https://github.com/lcirvine/strava_data/blob/cfa720274e5f312310a633a824a345375894a54e/reference/2020%20Time%20of%20Day%20Heatmap.png)
