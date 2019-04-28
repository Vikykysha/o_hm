
# # # coding: utf-8

# # # <img src="pics/otus.png">

# # # # Визуализация данных на Python

# # # In[1]:

# import numpy as np
# import pandas as pd


# # # In[ ]:




# # # In[2]:

# # data = pd.read_csv('populations.txt', sep='\t')
# # data.head()


# # # ## Библиотеки визуализации данных

# # # ## matplotlib
# # # 
# # # https://matplotlib.org/tutorials/index.html#introductory

# # # In[3]:

# # import matplotlib
# # import matplotlib.pyplot as plt 
# # import matplotlib.mlab as mlab
# # get_ipython().magic('matplotlib inline')


# # # In[4]:

# # fig, ax1 = plt.subplots(1, 1)


# # # In[5]:

# # fig, ax = plt.subplots(2, 2, figsize=(12,10))


# # # In[6]:

# # from matplotlib.ticker import ScalarFormatter, FormatStrFormatter

# # def offset_off(x):
# #     x.xaxis.set_major_formatter(FormatStrFormatter('%.0f'))


# # # In[7]:

# # fig, ax = plt.subplots(1,1)
# # ax.plot(data['year'], data['hare'])
# # offset_off(ax)


# # # In[8]:

# # fig, ax = plt.subplots(2, 2, figsize = (12,8))

# # ax[0][0].plot(data['year'], data['hare'], color ='#8c92ac')
# # offset_off(ax[0][0])

# # ax[0][1].plot(data['year'], data['lynx'], color = '#ffa500')
# # offset_off(ax[0][1])

# # ax[1][0].plot(data['year'], data['carrot'], color = '#b06500')
# # offset_off(ax[1][0])

# # ax[1][1].plot(data['year'], data['hare'], label = 'Hares', color='#8c92ac', ls = ':'); 
# # ax[1][1].plot(data['year'], data['lynx'], label = 'Lynxes', color='#ffa500', ls = '--');
# # ax[1][1].plot(data['year'], data['carrot'], label = 'Carrots', color='#b06500', ls = '-');
# # offset_off(ax[1][1])


# # # # Упражнение
# # # Реализовать отображение графика с четырьмя подокнами.
# # # На первых трех необходимо отобразить по отдельности популяции зайцев, рысей, моркови, и на четвертом отобразить их всех вместе. 
# # # 
# # # Проработать внешний вид графиков - data-ink ratio, согласованность, ясность - что есть что - , целостность и тд. Например, убедиться, что про каждый график известно, к чему он относится - зайцам, рысям или моркови. А так же убедиться, что для одних и тех же объектов используются одни и те же цвета.

# # # # Решение

# # # In[9]:

# # fig, ax1 = plt.subplots(2, 2, figsize=(12,8))

# # ax1[0][0].set_xlabel('Time', fontsize = 10)
# # ax1[0][0].set_ylabel('Hares', fontsize = 10)

# # ax1[0][0].plot(data['year'], data['hare'], color='#8c92ac', ls = ':'); 

# # ax1[0][0].spines['right'].set_visible(False)
# # ax1[0][0].spines['top'].set_visible(False)

# # ax1[0][0].yaxis.set_ticks_position('left')
# # ax1[0][0].xaxis.set_ticks_position('bottom')

# # for axis in ['top','bottom','left','right']:
# #     ax1[0][0].spines[axis].set_linewidth(0.5)

# # #Lynxes
# # ax1[0][1].set_xlabel('Time', fontsize = 10)
# # ax1[0][1].set_ylabel('Lynxes', fontsize = 10)

# # ax1[0][1].plot(data['year'], data['lynx'], color='#b06500', ls = '-'); 

# # ax1[0][1].spines['right'].set_visible(False)
# # ax1[0][1].spines['top'].set_visible(False)

# # ax1[0][1].yaxis.set_ticks_position('left')
# # ax1[0][1].xaxis.set_ticks_position('bottom')

# # for axis in ['top','bottom','left','right']:
# #     ax1[0][1].spines[axis].set_linewidth(0.5)
    
# # #Carrots
# # ax1[1][0].set_xlabel('Time', fontsize = 10)
# # ax1[1][0].set_ylabel('Carrots', fontsize = 10)

# # ax1[1][0].plot(data['year'], data['carrot'], color='#ffa500', ls = '--'); 

# # ax1[1][0].spines['right'].set_visible(False)
# # ax1[1][0].spines['top'].set_visible(False)

# # ax1[1][0].yaxis.set_ticks_position('left')
# # ax1[1][0].xaxis.set_ticks_position('bottom')

# # for axis in ['top','bottom','left','right']:
# #     ax1[1][0].spines[axis].set_linewidth(0.5)
    
# # # All of them
# # ax1[1][1].set_xlabel('Time', fontsize = 10)
# # ax1[1][1].set_ylabel('Trends in the Forest', fontsize = 10)

# # ax1[1][1].plot(data['year'], data['hare'], label = 'Hares', color='#8c92ac', ls = ':'); 
# # ax1[1][1].plot(data['year'], data['carrot'], label = 'Carrots', color='#ffa500', ls = '--');
# # ax1[1][1].plot(data['year'], data['lynx'], label = 'Lynxes', color='#b06500', ls = '-');
# # ax1[1][1].legend(loc=1, fontsize=10, frameon=False) # upper left corner


# # ax1[1][1].spines['right'].set_visible(False)
# # ax1[1][1].spines['top'].set_visible(False)

# # ax1[1][1].yaxis.set_ticks_position('left')
# # ax1[1][1].xaxis.set_ticks_position('bottom')

# # for axis in ['top','bottom','left','right']:
# #     ax1[1][1].spines[axis].set_linewidth(0.5)
 
# # offset_off(ax1[0][0])
# # offset_off(ax1[0][1])
# # offset_off(ax1[1][1])
# # offset_off(ax1[1][0])

# # fig.tight_layout()


# # # ### График можно сохранить в виде файла:

# # # In[10]:

# # fig.savefig("my_new_plot.png") 


# # # ### Доступные форматы, какие из них гарантируют сохранение лучшего качества?
# # # 
# # # 
# # # Matplotlib может сгенерировать результат высокого качества в разных форматах, в т.ч. PNG, JPG, EPS, SVG, PDF. Для научных статей рекомендуем использовать PDF везде, где это возможно. (В документы LaTeX, собираемые с помощью pdflatex, PDF  изображения можно включасть с помощью команды includegraphics).
# # # 
# # # 
# # # EPS, PDF, SVG - векторные форматы, что означает возможность редактирования изображения в программах подобных Adobe illustrator с сохранением возможности редактирования отдельных элементов изображения - линий, точек, текста и пр.
# # # 
# # # PNG, JPG - растровые форматы, как фото. В программах редактирования изображений, например, Adobe Illustrator, обычно доступен только один объект для редактирования.

# # # ## pandas
# # # https://pandas.pydata.org/pandas-docs/stable/visualization.html
# # # 
# # # * ‘bar’ or ‘barh’ for bar plots
# # # * ‘hist’ for histogram
# # # * ‘box’ for boxplot
# # # * ‘kde’ or 'density' for density plots
# # # * ‘area’ for area plots
# # # * ‘scatter’ for scatter plots
# # # * ‘hexbin’ for hexagonal bin plots
# # # * ‘pie’ for pie plots
# # # 

# # # ## Основные возможности Pandas
# # # 
# # # https://pandas.pydata.org/pandas-docs/stable/10min.html
# # # 

# # # In[11]:


# # dates = pd.date_range('20130101', periods=6)
# # dates


# # # In[12]:

# # df = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))
# # df


# # # ## view

# # # In[13]:

# # df.head()


# # # In[14]:

# # df.tail()


# # # In[15]:

# # df.index


# # # In[16]:

# # df.columns


# # # In[17]:

# # df.describe()


# # # ## select

# # # In[18]:

# # df['A']


# # # In[19]:

# # df[0:3]


# # # In[20]:

# # df[df.A > 0]


# # # In[21]:

# # df[(df.A > 0) & (df.B < 0)]


# # # In[22]:

# # df[df > 0]


# # # In[23]:

# # s = pd.Series(np.nan, index=[49,48,47,46,45, 1, 2, 3, 4, 5])
# # s


# # # In[24]:

# # s.iloc[:3]


# # # In[25]:

# # s.loc[:3]


# # # ## set

# # # In[26]:

# # df.at[dates[0],'A'] = 0
# # df


# # # In[27]:

# # df[df.A < 0] = 1
# # df


# # # In[28]:

# # df['E'] = 1
# # df


# # # ## calculate

# # # In[29]:

# # df.mean()


# # # In[30]:

# # df.max()


# # # In[31]:

# # df['A'].apply(lambda x: x + 1)


# # # ## join

# # # In[32]:

# # left = pd.DataFrame({'key': ['foo', 'foo'], 'lval': [1, 2]})
# # right = pd.DataFrame({'key': ['foo', 'foo'], 'rval': [4, 5]})


# # # In[33]:

# # left


# # # In[34]:

# # right


# # # In[35]:

# # pd.merge(left, right, on='key')


# # # ## group

# # # In[36]:

# # df = pd.DataFrame({'A' : ['foo', 'bar', 'foo', 'bar',
# #                           'foo', 'bar', 'foo', 'foo'],
# #                    'B' : ['one', 'one', 'two', 'three',
# #                           'two', 'two', 'one', 'three'],
# #                    'C' : np.random.randn(8),
# #                    'D' : np.random.randn(8)})
# # df


# # # In[37]:

# # df.groupby('A').sum()


# # # In[38]:

# # df.groupby(['A','B']).sum()


# # # ## pivot table

# # # In[39]:

# # df = pd.DataFrame({'A' : ['one', 'one', 'two', 'three'] * 3,
# #                    'B' : ['a', 'b', 'c'] * 4,
# #                    'C' : ['foo', 'foo', 'foo', 'bar', 'bar', 'bar'] * 2,
# #                    'D' : np.random.randn(12),
# #                    'E' : np.random.randn(12)})
# # df


# # # In[40]:

# # pd.pivot_table(df, values='D', index=['A', 'B'], columns=['C'])


# # # ## plot

# # # In[41]:

# # p = data.groupby('year').sum().plot(subplots=True, figsize=(20, 20), rot=0, sharey=True, legend=True)
# # offset_off(p[0])


# # # In[42]:

# # data.plot.bar(x='year')


# # # ## seaborn
# # # https://seaborn.pydata.org/

# # # In[43]:

# # import seaborn as sns


# # # In[44]:

# # sns.barplot(x='year', y='hare', data=data, palette="BuGn_d")


# # # ## plotly

# # # In[45]:


# # from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
# # init_notebook_mode()
# # import plotly.graph_objs as go


# # # In[46]:

# # hares = [
# #     go.Bar(
# #         x=data['year'],
# #         y=data['hare']
# #     )
# # ]

# # iplot(hares, filename='basic-bar')


# # # ## bokeh

# # # In[47]:

# # from bokeh.plotting import figure, output_file, show
# # from bokeh.io import output_notebook
# # from bokeh.charts import Bar
# # output_notebook()


# # # In[48]:

# # p = Bar(data, 'year', values='hare', title="Hare")
# # show(p)


# # # In[49]:

# # import pygal



# # # In[50]:

# # # from IPython.display import SVG, HTML

# # import pygal
# # from IPython.display import display

# # bar_chart = pygal.Bar()
# # bar_chart.add('Hare', data['hare'].values)
# # bar_chart.x_labels = map(str, data['year'].values)
# # display({'image/svg+xml': bar_chart.render()}, raw=True)


# # # ## Виды графиков

# # # ## Scatterplot

# # # In[51]:

# # fig, ax1= plt.subplots(1, 1)
# # ax1.plot(data['year'], data['hare'], 'o');


# # # In[52]:

# # data.plot.scatter(x='year', y='hare')


# # # ## Bar Charts (столбчатые диаграммы)

# # # In[53]:

# # fig, ax1= plt.subplots(1, 1)
# # width=0.2
# # ax1.bar(data['year'], data['hare'], width);  # параметр width изменяет ширину полосы


# # # In[54]:

# # data.plot.bar(x='year', y='hare')


# # # In[55]:

# # fig, ax1= plt.subplots(1, 1)
# # width=0.8
# # ax1.bar(data['year'], data['hare'], width, color='#98cff4'); 
# # ax1.bar(data['year'], data['lynx'], width, color='#ffe4e1', bottom=data['hare']);  # Если указать bottom, полосы будут отрисованы над указанными


# # # In[56]:

# # data.plot.bar(x='year', y=['hare', 'lynx', 'carrot'], stacked=True)


# # # ## Area plots (диаграммы областей)

# # # In[57]:

# # fig, ax1= plt.subplots(1, 1)

# # ax1.fill_between(data['year'], 0, data['hare'])
# # ax1.set_ylabel('Area between \n y=0 and hares')


# # # ## Stacked Area plots (составные диаграммы областей)

# # # In[58]:

# # fig, ax1= plt.subplots(1, 1)

# # ax1.stackplot(data['year'], data['hare'], data['lynx'], data['carrot'])
# # ax1.legend(['hares','lynxes','carrots'], frameon=False,loc='upper center');


# # # In[ ]:

# # data.plot.area(x='year')


# # # In[ ]:

# # data.plot.area(x='year', stacked=False)


# # # ## Grouped bar charts (сгруппированные столбчатые диаграммы)

# # # In[ ]:

# # hares5=data['hare'][0:5]
# # lynxes5=data['lynx'][0:5]
# # new_t5=data['year'][0:5]

# # fig, ax1= plt.subplots(1, 1, figsize=(12,5))
# # bar_width=0.3
# # hares_bar = ax1.bar(new_t5, hares5, bar_width,
# #                  color='b',
# #                  label='Hares')

# # lynxes_bar = ax1.bar(new_t5 + bar_width, lynxes5, bar_width,
# #                  color='r',
# #                  label='Lynxes')

# # ax1.set_xlabel('Year')
# # ax1.set_ylabel('Population')
# # plt.title('Population by Species')
# # plt.xticks(new_t5 + bar_width, ('1900', '1901', '1902', '1903', '1904'))
# # plt.legend();


# # # In[ ]:

# # data[data['year'] < 1905][['hare', 'lynx', 'year']].plot.bar(x='year', y=['hare', 'lynx'])


# # # ## Круговые диаграммы

# # # In[ ]:

# # fig = plt.figure()
# # ax = plt.axes([0.025,0.025,0.95,0.95], polar=True) # This is a different way to initialize the axes of a figure

# # N = len(data['year'])
# # theta = np.arange(0.0, 2*np.pi, 2*np.pi/N) #we need to map our time data to the angles in a circle
# # radii = data['hare'] 
# # mywidth = 0.3
# # bars = plt.bar(theta, radii, width=mywidth, bottom=0.0)

# # rmax=np.max(radii)

# # for r,bar in zip(radii, bars): 
# #     bar.set_facecolor( plt.cm.Pastel1_r(r/rmax)) #We are using here the colormap, which takes as input a number between 0 and 1
# #     bar.set_alpha(0.5) # With this we set the transparency of the plot. Try to put it equal to 1
    
# # ax.set_xticks(np.pi/180*np.linspace(0,  360, N+1))
# # ax.set_xticklabels((data['year'].astype(int)))
# # ax.set_yticklabels([])
# # plt.show()


# # # ## Distribution Plots (диаграммы распределения)

# # # In[ ]:

# # mu = 100.0
# # sigma1 = 15.0
# # A1 = np.random.normal(mu, sigma1, 10000) # Let's generate fake data, like IQ measurements


# # # In[ ]:

# # fig, ax= plt.subplots(1, 1)

# # # the histogram of the data
# # n, bins, patches = plt.hist(A1, 50, normed=1, facecolor='#368d5c', alpha=0.75)

# # plt.xlabel('Smarts')
# # plt.ylabel('Probability')
# # plt.title(r'$\mathrm{Histogram\ of\ IQ:}\ \mu=100,\ \sigma=15$')
# # plt.axis([40, 160, 0, 0.03])
# # plt.grid(True)

# # ax.spines["top"].set_visible(False)    
# # ax.spines["right"].set_visible(False)    
# # ax.yaxis.set_ticks_position('left')
# # ax.xaxis.set_ticks_position('bottom')

# # plt.show()


# # # In[ ]:

# # fig, ax= plt.subplots(1, 1)

# # # the histogram of the data
# # n, bins, patches = plt.hist(A1, 50, normed=1, facecolor='#368d5c', alpha=0.75)

# # # add a 'best fit' line
# # y = mlab.normpdf( bins, mu, sigma1)
# # l = plt.plot(bins, y, 'r--', linewidth=2)

# # plt.xlabel('Smarts')
# # plt.ylabel('Probability')
# # plt.title(r'$\mathrm{Histogram\ of\ IQ:}\ \mu=100,\ \sigma=15$')
# # plt.axis([40, 160, 0, 0.03])
# # plt.grid(True)

# # ax.spines["top"].set_visible(False)    
# # ax.spines["right"].set_visible(False)    
# # ax.yaxis.set_ticks_position('left')
# # ax.xaxis.set_ticks_position('bottom')

# # plt.show()


# # # In[ ]:

# # import seaborn as sns
# # p = sns.distplot(A1)

# # plt.xlabel('Smarts')
# # plt.ylabel('Probability')
# # plt.title(r'$\mathrm{Histogram\ of\ IQ:}\ \mu=100,\ \sigma=15$')
# # plt.axis([40, 160, 0, 0.03])
# # plt.grid(True)

# # ax.spines["top"].set_visible(False)    
# # ax.spines["right"].set_visible(False)    
# # ax.yaxis.set_ticks_position('left')
# # ax.xaxis.set_ticks_position('bottom')


# # # ## Density plot (диаграмма плотности распределения)

# # # In[ ]:

# # df = pd.DataFrame(A1)
# # ax = df.plot(kind='density', color = 'black')
# # ax.spines["top"].set_visible(False)    
# # ax.spines["right"].set_visible(False)
# # ax.yaxis.set_ticks_position('left')
# # ax.xaxis.set_ticks_position('bottom')
# # ax.legend().set_visible(False)


# # # ## Comparing distributions (сравнение распределений)

# # # In[ ]:

# # mu = 100.0
# # sigma1 = 15.0
# # sigma2 = 25.0
# # A1 = np.random.normal(mu, sigma1, 10000) # IQ measurements of humans
# # A2 = np.random.normal(mu, sigma2, 10000) # IQ measurements of aliens


# # # In[ ]:

# # fig, ax= plt.subplots(1, 1)

# # # the histogram of the data
# # n, bins, patches = plt.hist(A1, 50, normed=1, facecolor='#368d5c', alpha=0.75)
# # n, bins, patches = plt.hist(A2, 50, normed=1, facecolor='#efbb38', alpha=0.75)

# # plt.xlabel('Smarts')
# # plt.ylabel('Probability')
# # plt.title(r'$\mathrm{Histogram\ of\ IQ:}\ \mu=100,\ \sigma=15$')
# # plt.axis([40, 160, 0, 0.03])
# # plt.grid(True)

# # ax.spines["top"].set_visible(False)    
# # ax.spines["right"].set_visible(False)    
# # ax.yaxis.set_ticks_position('left')
# # ax.xaxis.set_ticks_position('bottom')

# # plt.show()


# # # ## Boxplots (ящик с усами)

# # # In[ ]:

# # fig, ax= plt.subplots(1, 1)
# # ax.boxplot([A1, A2]);
# # plt.setp(ax, xticklabels=['A1', 'A2']);


# # # In[ ]:

# # fig, ax= plt.subplots(1, 1)
# # bp=ax.boxplot([A1, A2]);
# # plt.setp(ax, xticklabels=['A1', 'A2'])
# # plt.setp(bp['boxes'], color='black')
# # plt.setp(bp['whiskers'], color='black')
# # plt.setp(bp['fliers'], marker='o', MarkerFaceColor='red');


# # # In[ ]:

# # mu = 100.0
# # sigma1 = 15.0
# # sigma2 = 25.0
# # A1 = np.random.normal(mu, sigma1, 10000) # IQ measurements of humans
# # A2 = np.concatenate((np.random.normal(mu-50, sigma1, 10000), np.random.normal(mu+50, sigma2, 10000)), axis=0) # IQ measurements of aliens


# # # In[ ]:

# # fig, ax= plt.subplots(1, 1)
# # ax.boxplot([A1, A2])
# # plt.setp(ax, xticklabels=['A1', 'A2']);


# # # In[ ]:

# # fig, ax= plt.subplots(1, 1)
# # ax.violinplot([A1, A2], showmeans=False, showmedians=True);


# # # In[ ]:

# # df=pd.read_csv('crimeRatesByState2005.tsv',header=0,sep='\t')


# # # In[ ]:

# # df.head()


# # # In[ ]:

# # state_data=df.as_matrix(columns=df.columns[1:])
# # state_names=np.array(df['state'])


# # # In[ ]:

# # fig, ax = plt.subplots(1, 1)
# # sc = plt.scatter(df['murder'], df['burglary'], s=df['population'] / 100000, c=df['motor_vehicle_theft'], alpha=0.5, cmap=plt.cm.get_cmap('viridis') )
# # plt.colorbar(sc)

# # # This figure is not final: you should put labels, title, units, remove the top and right axes if needed, add a grid, and so on.
# # # But you know how to do that!


# # # In[ ]:

# # fig, ax= plt.subplots(1, 1, figsize=(10,5))
# # plt.scatter(df['murder'], df['burglary'], s=df['population']/100000, c=df['motor_vehicle_theft'], alpha=0.5, cmap=plt.cm.get_cmap('viridis'))

# # for i in range(len(state_names)):
# #     ax.annotate(state_names[i], (state_data[i,0] + 0.2, state_data[i,4] + 10))
# # plt.colorbar(); 

# # # The result is a bit cluttered, but it is hard to untangle the text in an 
# # # automatic way. We can solve by using intereactive visualization


# # # In[ ]:

# # fig, ax = plt.subplots(1,1, figsize = (10,5))

# # state_names=np.array(df['state'])
# # x = np.array(df['murder'])
# # y = np.array(df['burglary'])
# # area = np.array(df['population']) / 30000
# # colours = np.array(df['Robbery'])
# # text = np.array(df['state'])
 
# # ax.scatter(x, y, s = area, c = colours, cmap = 'inferno', alpha = 0.5, linewidth = 0)

# # ax.set_xlabel('Murder', fontsize = 10)
# # ax.set_ylabel('Bulglarly', fontsize = 10)

# # for i, state in enumerate(text):
# #     ax.annotate(state, (x[i],y[i]), fontsize = 7) 


# # # In[ ]:




# # # In[ ]:

# # trace0 = go.Scatter(
# #     x=x,
# #     y=y,
# #     mode='markers',
# #     marker=dict(
# #         size=area,
# #         sizemode='area',
# #         sizemin=4,
# #         color=colours
# #     ),
# #     text=text
# # )

# # data = [trace0]
# # iplot(data, filename='bubblechart-size-ref')


# # # In[ ]:

# # from bokeh.plotting import figure, output_file, show
# # from bokeh.io import output_notebook
# # output_notebook()

# # p = figure()
# # p.scatter(x, y, radius=area / 1000., fill_color='black',  fill_alpha=0.8, line_color=None)

# # show(p)


# # # ## Качество визуализации для перезентации
# # # 
# # # * хорошая визуализация  
# # # NYT  
# # # http://www.nytimes.com/interactive/2009/03/01/business/20090301_WageGap.html  
# # # Блог plot.ly  
# # # https://plotlyblog.tumblr.com/  
# # # * плохая визуализация  
# # # http://viz.wtf/  

# # # In[ ]:

# # import seaborn as sns
# # sns.pairplot(df, kind="reg")


# # # In[ ]:

# # df.corr()


# # # ## Heatmap (тепловая карта)

# # # In[ ]:

# # nba = pd.read_csv('nba.csv', index_col=0)
# # # Normalize data columns
# # nba_norm = (nba - nba.mean()) / (nba.max() - nba.min())


# # # In[ ]:

# # nba_norm.head()


# # # In[ ]:

# # fig, ax = plt.subplots(1, 1, figsize=(15, 12))
# # ax.pcolor(nba_norm, cmap=plt.cm.get_cmap('Blues'), alpha=0.8)

# # # put the major ticks at the middle of each cell
# # ax.set_yticks(np.arange(nba_norm.shape[0]) + 0.5, minor=False)
# # ax.set_xticks(np.arange(nba_norm.shape[1]) + 0.5, minor=False)

# # # # want a more natural, table-like display
# # ax.invert_yaxis()
# # ax.xaxis.tick_top()

# # # Set the labels
# # # label source:https://en.wikipedia.org/wiki/Basketball_statistics
# # labels = ['Games','Minutes','Points','Field goals made','Field goal attempts','Field goal percentage','Free throws made','Free throws attempts','Free throws percentage','Three-pointers made','Three-point attempt','Three-point percentage','Offensive rebounds','Defensive rebounds','Total rebounds','Assists','Steals','Blocks','Turnover','Personal foul'];

# # # note I could have used nba_sort.columns but made "labels" instead
# # ax.set_xticklabels(labels, minor=False) 
# # ax.set_yticklabels(nba_norm.index, minor=False)

# # # rotate the 
# # t = plt.xticks(rotation=90)


# # # In[ ]:

# # trace = go.Heatmap(
# #     z=nba_norm.as_matrix(), 
# #     x=labels, 
# #     y=nba_norm.index, 
# #     colorscale='Blues', 
# #     opacity=0.8,
# # )
# # data=[trace]
# # iplot(data, filename='basic-heatmap')


# # # In[ ]:




# # # ## Работа с геоданными

# # # # Basemap
# # # https://matplotlib.org/basemap/

# # # In[ ]:

# # # Import the basemap package
# # from mpl_toolkits.basemap import Basemap
# # import matplotlib.pyplot as plt
# # airports = pd.read_csv('https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat', header=None, dtype=str)

# # airports.columns = ["id", "name", "city", "country", "code", "icao", "latitude", "longitude", "altitude", "offset", "dst", "timezone", 'dat1', 'dat2']

# # # Create a map on which to draw.  We're using a mercator projection, and showing the whole world.
# # m = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')
# # # Draw coastlines, and the edges of the map.
# # m.drawcoastlines()
# # m.drawmapboundary()
# # # Convert latitude and longitude to x and y coordinates
# # x, y = m(list(airports["longitude"].astype(float)), list(airports["latitude"].astype(float)))
# # # Use matplotlib to draw the points onto the map.
# # m.scatter(x,y,1,marker='o',color='red')
# # # Show the plot.
# # plt.show()


# # # In[ ]:

# # # Make a base map with a mercator projection.  Draw the coastlines.
# # routes = pd.read_csv('https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat', header=None, dtype=str)
# # routes.columns = ["airline", "airline_id", "source", "source_id", "dest", "dest_id", "codeshare", "stops", "equipment"]

# # m = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')
# # m.drawcoastlines()

# # # Iterate through the first 3000 rows.
# # for name, row in routes[:3000].iterrows():
# #     try:
# #         # Get the source and dest airports.
# #         source = airports[airports["id"] == row["source_id"]].iloc[0]
# #         dest = airports[airports["id"] == row["dest_id"]].iloc[0]
# #         # Don't draw overly long routes.
# #         if abs(float(source["longitude"]) - float(dest["longitude"])) < 90:
# #             # Draw a great circle between source and dest airports.
# #             m.drawgreatcircle(float(source["longitude"]), float(source["latitude"]), float(dest["longitude"]), float(dest["latitude"]),linewidth=1,color='b')
# #     except (ValueError, IndexError):
# #         pass
    
# # # Show the map.
# # plt.show()


# # # In[ ]:

# # import sklearn.datasets.california_housing as ch
# # dataset = ch.fetch_california_housing()

# # X = dataset.data
# # Y = dataset.target

# # plt.figure(figsize=(10, 10))

# # lllon, lllat, urlon, urlat = X[:, -1].min(), X[:, -2].min(), X[:, -1].max(), X[:, -2].max()

# # m = Basemap(
# #     llcrnrlon=lllon,
# #     llcrnrlat=lllat,
# #     urcrnrlon=urlon,
# #     urcrnrlat=urlat, 
# #     projection='merc',
# #     resolution='h'
# # )

# # m.drawcoastlines(linewidth=0.5)
# # m.drawmapboundary(fill_color='#47A4C9', zorder=1)
# # m.fillcontinents(color='#88D8B0',lake_color='#47A4C9', zorder=2)

# # parallels = np.linspace(lllat, urlat, 10)
# # m.drawparallels(parallels,labels=[1,0,0,0],fontsize=10)
# # # draw meridians
# # meridians = np.linspace(lllon, urlon, 10)
# # m.drawmeridians(meridians,labels=[0,0,0,1],fontsize=10)

# # colors = [plt.cm.hot(int((y - Y.min()) / (Y.max() - Y.min()) * 256)) for y in Y]
# # m.scatter(X[:, -1], X[:, -2], latlon=True, zorder=3, lw=0, c=colors)

# # plt.annotate('San Francisco', xy=(0.04, 0.5), xycoords='axes fraction', color='white', size=15)
# # plt.annotate('Los Angeles', xy=(0.4, 0.08), xycoords='axes fraction', color='white', size=15)

# # plt.show()


# # # # Folium
# # # https://github.com/python-visualization/folium

# # # In[ ]:

# # import folium


# # m = folium.Map(location=[45.5236, -122.6750])
# # m


# # # In[ ]:

# # import folium

# # # Get a basic world map.
# # airports_map = folium.Map(location=[30, 0], zoom_start=2)
# # # # Draw markers on the map.
# # for idx, (name, row) in enumerate(airports.iterrows()):
# #     folium.Marker([float(row["latitude"]), float(row["longitude"])], popup=row["name"]).add_to(airports_map)
# #     if idx > 100:
# #         break
        
# # # Create and show the map.
# # airports_map


# # ## geoplotlib
# # https://github.com/andrea-cuttone/geoplotlib

# # In[ ]:

import geoplotlib
from geoplotlib.utils import read_csv


data = read_csv('flights.csv')
geoplotlib.graph(data,
                 src_lat='lat_departure',
                 src_lon='lon_departure',
                 dest_lat='lat_arrival',
                 dest_lon='lon_arrival',
                 color='hot_r',
                 alpha=16,
                 linewidth=2)
geoplotlib.show()


# # # ## networkx + plotly
# # # 
# # # https://networkx.github.io/
# # # https://plot.ly/python/network-graphs/

# # # In[ ]:

# # import plotly.plotly as py
# # from plotly.graph_objs import *

# # import networkx as nx

# # G=nx.random_geometric_graph(200,0.125)
# # pos=nx.get_node_attributes(G,'pos')

# # dmin=1
# # ncenter=0
# # for n in pos:
# #     x,y=pos[n]
# #     d=(x-0.5)**2+(y-0.5)**2
# #     if d<dmin:
# #         ncenter=n
# #         dmin=d

# # p=nx.single_source_shortest_path_length(G,ncenter)

# # edge_trace = Scatter(
# #     x=[],
# #     y=[],
# #     line=Line(width=0.5,color='#888'),
# #     hoverinfo='none',
# #     mode='lines')

# # for edge in G.edges():
# #     x0, y0 = G.node[edge[0]]['pos']
# #     x1, y1 = G.node[edge[1]]['pos']
# #     edge_trace['x'] += [x0, x1, None]
# #     edge_trace['y'] += [y0, y1, None]

# # node_trace = Scatter(
# #     x=[],
# #     y=[],
# #     text=[],
# #     mode='markers',
# #     hoverinfo='text',
# #     marker=Marker(
# #         showscale=True,
# #         # colorscale options
# #         # 'Greys' | 'Greens' | 'Bluered' | 'Hot' | 'Picnic' | 'Portland' |
# #         # Jet' | 'RdBu' | 'Blackbody' | 'Earth' | 'Electric' | 'YIOrRd' | 'YIGnBu'
# #         colorscale='YIGnBu',
# #         reversescale=True,
# #         color=[],
# #         size=10,
# #         colorbar=dict(
# #             thickness=15,
# #             title='Node Connections',
# #             xanchor='left',
# #             titleside='right'
# #         ),
# #         line=dict(width=2)))

# # for node in G.nodes():
# #     x, y = G.node[node]['pos']
# #     node_trace['x'].append(x)
# #     node_trace['y'].append(y)
    
    
# # for node, adjacencies in enumerate(G.adjacency()):
# #     node_trace['marker']['color'].append(len(adjacencies))
# #     node_info = '# of connections: '+str(len(adjacencies))
# #     node_trace['text'].append(node_info)
    
# # fig = Figure(data=Data([edge_trace, node_trace]),
# #              layout=Layout(
# #                 title='<br>Network graph made with Python',
# #                 titlefont=dict(size=16),
# #                 showlegend=False,
# #                 hovermode='closest',
# #                 margin=dict(b=20,l=5,r=5,t=40),
# #                 annotations=[ dict(
# #                     text="Python code: <a href='https://plot.ly/ipython-notebooks/network-graphs/'> https://plot.ly/ipython-notebooks/network-graphs/</a>",
# #                     showarrow=False,
# #                     xref="paper", yref="paper",
# #                     x=0.005, y=-0.002 ) ],
# #                 xaxis=XAxis(showgrid=False, zeroline=False, showticklabels=False),
# #                 yaxis=YAxis(showgrid=False, zeroline=False, showticklabels=False)))

# # iplot(fig, filename='networkx')


# # # In[ ]:



