import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import numpy as np
import seaborn as sns
import matplotlib.patches
import scipy.stats as stats
from datetime import datetime
plt.style.use('ggplot')

def objectivePercent(df, ax=None, title=''):
    #assigning ax variable if not given for plotting
    if ax is None:
        ax = plt.gca()
        
    #calculate percentages of scores with/without objectivity (neutral value =/!= 1)
    objective = pd.crosstab(df['date'].astype(str), df['neu'] == 1)
    objective_rate = objective.div(objective.sum(1).astype(float), axis=0)
    
    #plot objective_rate percentages in a stacked bar graph
    objective_rate.plot(kind='barh', ax=ax, stacked=True, color=['blue','gray'])
    
    #objective bar plot formatting
    ax.legend(['Objective', 'Neutral'], loc=1)
    ax.set_xlabel('Percentage', color='k')
    ax.set_ylabel('Date', color='k')
    ax.tick_params('both', colors='k')
    ax.set_title(title)
    ax.invert_yaxis()
    plt.tight_layout()
    
def objectiveDistribution(df_obj, df_polar, ax=None, title=''):   
    #assigning ax variable if not given for plotting
    if ax is None:
        ax = plt.gca()
        
    #generate distribution plot for each set of values following a beta distribution
    sns.distplot(df_polar['value'].abs(), ax=ax, fit_kws={'color':'blue'}, fit=stats.beta, hist=None, kde=False)
    sns.distplot(df_obj['neu'], ax=ax, fit_kws={'color':'gray'}, fit=stats.beta, hist=None, kde=False)
    
    #get plotted lines for shading
    l1 = ax.lines[0]
    l2 = ax.lines[1]

    #get xy coordinate data from each plotted line for shading
    x1 = l1.get_xydata()[:,0]
    y1 = l1.get_xydata()[:,1]
    x2 = l2.get_xydata()[:,0]
    y2 = l2.get_xydata()[:,1]
    
    #shade between the two sets of xy coordinate data from each plotted line
    ax.fill_between(x1,y1, color="blue", alpha=0.3)
    ax.fill_between(x2,y2, color="gray", alpha=0.3)

    #objective distribution plot formatting
    ax.legend(['Objective', 'Neutral'], loc=1)
    ax.set_xlabel('Percentage', color='k')
    ax.tick_params('both', colors='k')
    ax.yaxis.set_visible(False)
    ax.set_title(title)
    plt.tight_layout()    

def dailyPriceSentimentChart(df, ax=None, ax_dual=None, title=''):
    #assigning ax variable if not given for plotting
    if ax is None:
        ax = plt.gca()
    
    #plot price_diff series against date
    ax.plot(df.date, df.price_diff, 'b.:', markersize=20, markerfacecolor="None")

    #determine maximum distance away from zero as factor of 100 (for y axis labels)
    max_diff = np.ceil(max(np.abs(ax.get_ybound()))/100)*100
    
    #get the properly spaced xlabels determined by seaborn
    xlabels = ax.get_xticklabels()

    #price_diff series plot formatting
    ax.set_title(title, color='k')
    ax.set_xlabel('Date', color = 'k')
    ax.tick_params('x',labelrotation=-45)
    ax.set_xticklabels(xlabels, ha="left")
    ax.tick_params('x', colors='k')
    ax.set_ylabel('Price Change (USD)', color='b')
    ax.tick_params('y', colors='b')
    ax.yaxis.set_ticks(np.linspace(-max_diff,max_diff,5))

    #new plot with matching x axis as price_diff
    ax_dual = ax.twinx()

    #plot horizontal line to show +/- boundary
    horiz_line = np.array([0 for i in range(len(df.date))])
    ax_dual.plot(df.date,horiz_line, 'k')

    #plot daily_sentiment series against date
    ax_dual.plot(df.date, df.value, 'ms:', markersize=10, markerfacecolor="None")

    #determine maximum distance away from zero as factor of 10 (for y axis labels)
    max_pol = np.ceil(max(np.abs(ax_dual.get_ybound()))*10)/10

    #daily_sentiment series plot formatting
    ax_dual.set_ylabel('Average Polarity', color='m')
    ax_dual.tick_params('y', colors='m')
    ax_dual.yaxis.set_ticks(np.linspace(-max_pol,max_pol,5))

    #custom legend for both plots
    blue_circle = mlines.Line2D([],[], color='b', marker='.', markersize=10, markerfacecolor="None", label='Price', linestyle=":")
    pink_square = mlines.Line2D([],[], color='m', marker='s', markersize=5, markerfacecolor="None", label='Polarity', linestyle=":")
    plt.legend(handles=[blue_circle,pink_square], loc = "lower right")
    
    #plot formatting to prevent subplot overlap
    plt.tight_layout()
    
    

def dailySentimentsOverview(df, ax1=None, ax2=None, title=''):
    #assigning ax variable if not given for plotting
    if ax1 is None:
        #create figure with subplots that share a x axis and properly sized to one another
        fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(8,6), gridspec_kw={'height_ratios':[3,1]}) 
    
    #converting all negative polarity values to positive numbers for plotting
    df.value = abs(df.value)

    #custom colors for +/- values
    colors = ['green', 'red']

    #plot first subplot, boxplots for +/- polarity values by day
    sns.boxplot(data=df, x='date', y='value', palette=colors, hue='variable', ax=ax1)
    
    #polarity boxplot formatting
    ax1.set_title(title)
    ax1.set_ylabel('Polarity', color = 'k')
    ax1.tick_params('y', colors='k')
    ax1.xaxis.set_visible(False)
    ax1.legend_.set_title(None)
    ax1.legend(loc=1)

    #looping through all boxplot chart elements to remove color inside boxplot boxes and change all
    #line and marker elements to proper color according to +/- polarity group
    for i,artist in enumerate(ax1.artists):
        col = artist.get_facecolor()
        artist.set_edgecolor(col)
        artist.set_facecolor('None')
    
        for j in range(i*6,i*6+6):
            line = ax1.lines[j]
            line.set_color(col)
            line.set_mfc(col)
            line.set_mec(col)

    #plot second subplot, bar chart showing number of +/- polarity values by day
    df.date = df.date.astype(str) #convert date objects to strings
    sns.countplot(data=df, x='date', palette=colors, hue='variable', linewidth=2, ax=ax2)
    
    #get the properly spaced xlabels determined by seaborn
    xlabels = ax2.get_xticklabels()

    #polarity bar chart formatting
    ax2.set_xlabel('Date', color = 'k')
    ax2.tick_params('x',labelrotation=-45)
    ax2.set_xticklabels(xlabels, ha="left")
    ax2.tick_params('both', colors='k')
    ax2.set_ylabel('Count', color = 'k')
    ax2.legend_.remove()

    #looping through all bar chart elements to remove color inside bars and change its edges to
    #proper color according to +/- polarity values
    back_col = ax2.get_facecolor() #chart background color
    for child in ax2.get_children():
        if isinstance(child, matplotlib.patches.Rectangle):
            col = child.get_facecolor()
            if not col == back_col: #checking to make sure Rectangle is bar and not axes Rectangles
                child.set_edgecolor(col)
                child.set_facecolor('None')

    #tighten subplot spacing
    plt.subplots_adjust(hspace=.03)
       
def dualAxisTimeSeries(df, x, y1, y2, ax=None, ax_dual=None, title='', xlabel='', y1label='', y2label='', MA_line=False, y1_window=None, y2_window=None, horz_line=False):
    #assigning ax variable if not given for plotting
    if ax is None:
        ax = plt.gca()
    
    #plot price_diff series against date
    ax.plot(df[x], df[y1], 'b:')
    
    if MA_line is True:
        ax.plot(df[x], df[y1].rolling(window=y1_window).mean(), 'b-')

    #determine maximum distance away from zero as factor of 100 (for y axis labels)
    max_diff = np.ceil(max(np.abs(ax.get_ybound()))/100)*100
    
    #get the properly spaced xlabels determined by seaborn
    xlabels = ax.get_xticklabels()

    #price_diff series plot formatting
    ax.set_title(title, color='k')
    ax.set_xlabel(xlabel, color = 'k')
    ax.tick_params('x',labelrotation=-45)
    ax.set_xticklabels(xlabels, ha="left")
    ax.tick_params('x', colors='k')
    ax.set_ylabel(y1label, color='b')
    ax.tick_params('y', colors='b')
    ax.yaxis.set_ticks(np.linspace(0,max_diff,5))

    #new plot with matching x axis as price_diff
    ax_dual = ax.twinx()

    #plot horizontal line to show +/- boundary
    if horz_line is True:
        horiz_line = np.array([0 for i in range(len(df[x]))])
        ax_dual.plot(df[x],horiz_line, 'k')

    #plot daily_sentiment series against date
    ax_dual.plot(df[x], df[y2], 'm:')
    
    if MA_line is True:
        ax_dual.plot(df[x], df[y2].rolling(window=y2_window).mean(), 'm-')

    #determine maximum distance away from zero as factor of 10 (for y axis labels)
    max_pol = np.ceil(max(np.abs(ax_dual.get_ybound()))*10)/10

    #daily_sentiment series plot formatting
    ax_dual.set_ylabel(y2label, color='m')
    ax_dual.tick_params('y', colors='m')
    ax_dual.yaxis.set_ticks(np.linspace(0,max_pol,5))

    #custom legend for both plots
    blue_circle = mlines.Line2D([],[], color='b', label=y1, linestyle=":")
    pink_square = mlines.Line2D([],[], color='m', label=y2, linestyle=":")
    
    if MA_line is True:
        blue_line = mlines.Line2D([],[], color='b', label=y1+" (MA="+str(y1_window)+")", linestyle="-")
        pink_line = mlines.Line2D([],[], color='m', label=y2+" (MA="+str(y2_window)+")", linestyle="-")
        
        plt.legend(handles=[blue_circle,blue_line,pink_square,pink_line], loc = "upper left")
    else:
        plt.legend(handles=[blue_circle,pink_square], loc = "upper left")
    
    #plot formatting to prevent subplot overlap
    plt.tight_layout()